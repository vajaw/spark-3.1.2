/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.client;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamFailure;
import org.apache.spark.network.protocol.StreamResponse;
import org.apache.spark.network.server.MessageHandler;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.apache.spark.network.util.TransportFrameDecoder;

//*处理服务器响应，以响应[[TransportClient]]发出的请求的处理程序。它通过跟踪未完成的请求（及其回调）列表来工作。
//*并发：线程安全，可以从多个线程调用。
/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
  private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private final Channel channel;

  private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

  private final Map<Long, RpcResponseCallback> outstandingRpcs;

  private final Queue<Pair<String, StreamCallback>> streamCallbacks;
  private volatile boolean streamActive;

  /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
  // 记录上次发送fetch或RPC请求的时间（以系统纳秒为单位）
  private final AtomicLong timeOfLastRequestNs;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingFetches = new ConcurrentHashMap<>();
    this.outstandingRpcs = new ConcurrentHashMap<>();
    this.streamCallbacks = new ConcurrentLinkedQueue<>();
    this.timeOfLastRequestNs = new AtomicLong(0);
  }

  // 添加一个Fetch请求的回调 ？？何时调用？？
  public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
    updateTimeOfLastRequest();
    outstandingFetches.put(streamChunkId, callback);
  }

  public void removeFetchRequest(StreamChunkId streamChunkId) {
    outstandingFetches.remove(streamChunkId);
  }

  // 添加一个RPC请求的回调？？ 何时调用
  public void addRpcRequest(long requestId, RpcResponseCallback callback) {
    updateTimeOfLastRequest();
    outstandingRpcs.put(requestId, callback);
  }

  public void removeRpcRequest(long requestId) {
    outstandingRpcs.remove(requestId);
  }

  // 添加一个Stream 请求的回调？？ 何时调用
  public void addStreamCallback(String streamId, StreamCallback callback) {
    updateTimeOfLastRequest();
    streamCallbacks.offer(ImmutablePair.of(streamId, callback));
  }

  @VisibleForTesting
  public void deactivateStream() {
    streamActive = false;
  }

  /**
   * Fire the failure callback for all outstanding requests. This is called when we have an
   * uncaught exception or pre-mature connection termination.
   */
  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
      try {
        entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
      } catch (Exception e) {
        logger.warn("ChunkReceivedCallback.onFailure throws exception", e);
      }
    }
    for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
      try {
        entry.getValue().onFailure(cause);
      } catch (Exception e) {
        logger.warn("RpcResponseCallback.onFailure throws exception", e);
      }
    }
    for (Pair<String, StreamCallback> entry : streamCallbacks) {
      try {
        entry.getValue().onFailure(entry.getKey(), cause);
      } catch (Exception e) {
        logger.warn("StreamCallback.onFailure throws exception", e);
      }
    }

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingFetches.clear();
    outstandingRpcs.clear();
    streamCallbacks.clear();
  }

  @Override
  public void channelActive() {
  }

  // 一、日志定位的两条思路
  // 1、何时会调用此方法
  @Override
  public void channelInactive()  {
    // 2、为什么未完成的请求会大于0
    if (numOutstandingRequests() > 0) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  @Override
  public void handle(ResponseMessage message) throws Exception {
    if (message instanceof ChunkFetchSuccess) {
      ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel));
        resp.body().release();
      } else {
        outstandingFetches.remove(resp.streamChunkId); // 无论服务器响应streamChunk成功还是失败，
                                                        // 都会从streamChunk这个map中移除该streamChunkId
        listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
        resp.body().release();
      }
    } else if (message instanceof ChunkFetchFailure) {
      ChunkFetchFailure resp = (ChunkFetchFailure) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel), resp.errorString);
      } else {
        outstandingFetches.remove(resp.streamChunkId); // 无论服务器响应streamChunk成功还是失败，
                                                        // 都会从streamChunk这个map中移除该streamChunkId
        listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
          "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
      }
    } else if (message instanceof RpcResponse) {
      RpcResponse resp = (RpcResponse) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
          resp.requestId, getRemoteAddress(channel), resp.body().size());
        resp.body().release();
      } else {
        outstandingRpcs.remove(resp.requestId); // 无论服务器响应RPC成功还是失败，
                                                  // 都会从outstandingRpcs这个map中移除该requestId
        try {
          listener.onSuccess(resp.body().nioByteBuffer());
        } finally {
          resp.body().release();
        }
      }
    } else if (message instanceof RpcFailure) {
      RpcFailure resp = (RpcFailure) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
          resp.requestId, getRemoteAddress(channel), resp.errorString);
      } else {
        outstandingRpcs.remove(resp.requestId); // 无论服务器响应RPC成功还是失败，
                                                  // 都会从outstandingRpcs这个map中移除该requestId
        listener.onFailure(new RuntimeException(resp.errorString));
      }
    } else if (message instanceof StreamResponse) {
      StreamResponse resp = (StreamResponse) message;
      Pair<String, StreamCallback> entry = streamCallbacks.poll(); //queue.poll() 检索但不移除此队列的头
      if (entry != null) {
        StreamCallback callback = entry.getValue();
        if (resp.byteCount > 0) {
          StreamInterceptor<ResponseMessage> interceptor = new StreamInterceptor<>(
            this, resp.streamId, resp.byteCount, callback);
          try {
            TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
              channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
            frameDecoder.setInterceptor(interceptor);
            streamActive = true;
          } catch (Exception e) {
            logger.error("Error installing stream handler.", e);
            deactivateStream();
          }
        } else {
          try {
            callback.onComplete(resp.streamId);
          } catch (Exception e) {
            logger.warn("Error in stream handler onComplete().", e);
          }
        }
      } else {
        logger.error("Could not find callback for StreamResponse.");
      }
    } else if (message instanceof StreamFailure) {
      StreamFailure resp = (StreamFailure) message;
      Pair<String, StreamCallback> entry = streamCallbacks.poll();
      if (entry != null) {
        StreamCallback callback = entry.getValue();
        try {
          callback.onFailure(resp.streamId, new RuntimeException(resp.error));
        } catch (IOException ioe) {
          logger.warn("Error in stream failure handler.", ioe);
        }
      } else {
        logger.warn("Stream failure with unknown callback: {}", resp.error);
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  // 未完成的请求数，站在客户端的角度来看，服务端有两种情况，第一种情况：服务端返回响应，并发送了执行成功
  // 或者执行失败的消息，另一种情况服务端还没有完成，完成后才返回响应。
  /** Returns total number of outstanding requests (fetch requests + rpcs) */
  public int numOutstandingRequests() {
    return outstandingFetches.size() + outstandingRpcs.size() + streamCallbacks.size() +
      (streamActive ? 1 : 0);
  }

  /** Returns the time in nanoseconds of when the last request was sent out. */
  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }

  // 客户端最后一次发送请求的时间设置为当前时间。
  /** Updates the time of the last request to the current system time. */
  public void updateTimeOfLastRequest() {
    timeOfLastRequestNs.set(System.nanoTime());
  }

}
