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

package org.apache.spark.network.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.spark.network.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.protocol.ResponseMessage;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * The single Transport-level Channel handler which is used for delegating requests to the
 * {@link TransportRequestHandler} and responses to the {@link TransportResponseHandler}.
 *
 * All channels created in the transport layer are bidirectional. When the Client initiates a Netty
 * Channel with a RequestMessage (which gets handled by the Server's RequestHandler), the Server
 * will produce a ResponseMessage (handled by the Client's ResponseHandler). However, the Server
 * also gets a handle on the same Channel, so it may then begin to send RequestMessages to the
 * Client.
 * This means that the Client also needs a RequestHandler and the Server needs a ResponseHandler,
 * for the Client's responses to the Server's requests.
 *
 * This class also handles timeouts from a {@link io.netty.handler.timeout.IdleStateHandler}.
 * We consider a connection timed out if there are outstanding fetch or RPC requests but no traffic
 * on the channel for at least `requestTimeoutMs`. Note that this is duplex traffic; we will not
 * timeout if the client is continuously sending but getting no responses, for simplicity.
 */

// 单个 Transport-level Channel handler，用于将请求授权给 TransportRequestHandler
//                                         将响应授权给 TransportResponseHandler

// 在传输层中创建的所有Channel都是双向的。当客户端使用RequestMessage启动Netty Channel时（由服务器的RequesntHadler处理），
// 服务器将生成ResponseMessage（由客户端的Response Handler进行处理）。 然而，服务器也在同一通道上获得一个句柄，
// 因此它可能会开始向Client发送RequestMessages。
// 针对client对服务器请求的响应，这意味着client也需要一个RequestHandler，Server也需要一个ResponseHandler。

// TransportChannelHandler还处理来自{@link-io.netty.handler.timeout.IdleStateHandler}的timeouts。
// 如果存在 outstanding 的fetch或RPC请求，但通道在“requestTimeoutMs”所配置的延迟时间内没有流量，则认为连接超时。
// 请注意，这是双向通讯的流量；为了简单起见，如果客户端持续发送但没有收到响应，我们不会超时。

// 原理上，也就是说客户端和服务端之间没有通讯，但是还建立了一个channal，才会超时。
// 那什么情况下服务端和客户端才没有通讯？？？？？
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
  private static final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

  private final TransportClient client;
  private final TransportResponseHandler responseHandler;
  private final TransportRequestHandler requestHandler;
  private final long requestTimeoutNs;
  private final boolean closeIdleConnections;
  private final boolean skipChunkFetchRequest;
  private final TransportContext transportContext;

  public TransportChannelHandler(
      TransportClient client,
      TransportResponseHandler responseHandler,
      TransportRequestHandler requestHandler,
      long requestTimeoutMs,
      boolean skipChunkFetchRequest,
      boolean closeIdleConnections,
      TransportContext transportContext) {
    this.client = client;
    this.responseHandler = responseHandler;
    this.requestHandler = requestHandler;
    this.requestTimeoutNs = requestTimeoutMs * 1000L * 1000;
    this.skipChunkFetchRequest = skipChunkFetchRequest;
    this.closeIdleConnections = closeIdleConnections;
    this.transportContext = transportContext;
  }

  public TransportClient getClient() {
    return client;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Exception in connection from " + getRemoteAddress(ctx.channel()),
      cause);
    requestHandler.exceptionCaught(cause);
    responseHandler.exceptionCaught(cause);
    ctx.close();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    try {
      requestHandler.channelActive();
    } catch (RuntimeException e) {
      logger.error("Exception from request handler while channel is active", e);
    }
    try {
      responseHandler.channelActive();
    } catch (RuntimeException e) {
      logger.error("Exception from response handler while channel is active", e);
    }
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    try {
      requestHandler.channelInactive();
    } catch (RuntimeException e) {
      logger.error("Exception from request handler while channel is inactive", e);
    }
    try {
      responseHandler.channelInactive();
    } catch (RuntimeException e) {
      logger.error("Exception from response handler while channel is inactive", e);
    }
    super.channelInactive(ctx);
  }

  /**
   * Overwrite acceptInboundMessage to properly delegate ChunkFetchRequest messages
   * to ChunkFetchRequestHandler.
   *
   * 覆盖acceptInboundMessage以将ChunkFetchRequest消息正确委托给ChunkFetchRequestHandler。
   */
  @Override
  public boolean acceptInboundMessage(Object msg) throws Exception {
    if (skipChunkFetchRequest && msg instanceof ChunkFetchRequest) {
      return false;
    } else {
      return super.acceptInboundMessage(msg);
    }
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Message request) throws Exception {
    if (request instanceof RequestMessage) {
      requestHandler.handle((RequestMessage) request);
    } else if (request instanceof ResponseMessage) {
      responseHandler.handle((ResponseMessage) request);
    } else {
      ctx.fireChannelRead(request);
    }
  }

  /**基于{@link io.netty.handler.timeout.IdleStateHandler}中的事件触发*/
  /** Triggered based on events from an {@link io.netty.handler.timeout.IdleStateHandler}. */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      // See class comment for timeout semantics. In addition to ensuring we only timeout while
      // there are outstanding requests, we also do a secondary consistency check to ensure
      // there's no race between the idle timeout and incrementing the numOutstandingRequests
      // (see SPARK-7003).
      //
      // To avoid a race between TransportClientFactory.createClient() and this code which could
      // result in an inactive client being returned, this needs to run in a synchronized block.
      synchronized (this) {
        // 是否有未完成的线程
        boolean hasInFlightRequests = responseHandler.numOutstandingRequests() > 0;
        boolean isActuallyOverdue =
          System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;

        // IdleState.ALL_IDLE 有一段时间没有收到或发送数据
        // 并且客户端客户端的等待时间超过了配置的 spark.shuffle.io.connectionTimeout
        if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
          if (hasInFlightRequests) {
            // 如果线程长时间没有收发数据，并且客户端的等待时间时间超过了getOrDefault(spark.network.timeout,
            // spark.shuffle.io.connectionTimeout) 所设置的时间将
            String address = getRemoteAddress(ctx.channel());
            logger.error("Connection to {} has been quiet for {} ms while there are outstanding " +
              "requests. Assuming connection is dead; please adjust spark.network.timeout if " +
              "this is wrong.", address, requestTimeoutNs / 1000 / 1000);
            client.timeOut();
            ctx.close();
          } else if (closeIdleConnections) {
            // While CloseIdleConnections is enable, we also close idle connection
            client.timeOut();
            ctx.close();
          }
        }
      }
    }
    ctx.fireUserEventTriggered(evt);
  }

  public TransportResponseHandler getResponseHandler() {
    return responseHandler;
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    transportContext.getRegisteredConnections().inc();
    super.channelRegistered(ctx);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    transportContext.getRegisteredConnections().dec();
    super.channelUnregistered(ctx);
  }

}
