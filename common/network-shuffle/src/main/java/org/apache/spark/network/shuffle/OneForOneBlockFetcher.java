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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.TransportConf;

/**
 * Simple wrapper on top of a TransportClient which interprets each chunk as a whole block, and
 * invokes the BlockFetchingListener appropriately. This class is agnostic to the actual RPC
 * handler, as long as there is a single "open blocks" message which returns a ShuffleStreamHandle,
 * and Java serialization is used.
 *
 * Note that this typically corresponds to a
 * {@link org.apache.spark.network.server.OneForOneStreamManager} on the server side.
 */
public class OneForOneBlockFetcher {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneBlockFetcher.class);

  private final TransportClient client;
  private final BlockTransferMessage message;
  private final String[] blockIds;
  private final BlockFetchingListener listener;
  private final ChunkReceivedCallback chunkCallback;
  private final TransportConf transportConf;
  private final DownloadFileManager downloadFileManager;

  private StreamHandle streamHandle = null;

  public OneForOneBlockFetcher(
    TransportClient client,
    String appId,
    String execId,
    String[] blockIds,
    BlockFetchingListener listener,
    TransportConf transportConf) {
    this(client, appId, execId, blockIds, listener, transportConf, null);
  }

  public OneForOneBlockFetcher(
      TransportClient client,
      String appId,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener,
      TransportConf transportConf,
      DownloadFileManager downloadFileManager) {
    this.client = client;
    this.listener = listener;
    this.chunkCallback = new ChunkCallback();
    this.transportConf = transportConf;
    this.downloadFileManager = downloadFileManager;
    if (blockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }
    if (!transportConf.useOldFetchProtocol() && isShuffleBlocks(blockIds)) {
      this.blockIds = new String[blockIds.length];
      this.message = createFetchShuffleBlocksMsgAndBuildBlockIds(appId, execId, blockIds);
    } else {
      this.blockIds = blockIds;
      this.message = new OpenBlocks(appId, execId, blockIds);
    }
  }

  private boolean isShuffleBlocks(String[] blockIds) {
    for (String blockId : blockIds) {
      if (!blockId.startsWith("shuffle_")) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create FetchShuffleBlocks message and rebuild internal blockIds by
   * analyzing the pass in blockIds.
   */
  private FetchShuffleBlocks createFetchShuffleBlocksMsgAndBuildBlockIds(
      String appId, String execId, String[] blockIds) {
    String[] firstBlock = splitBlockId(blockIds[0]);
    int shuffleId = Integer.parseInt(firstBlock[1]);
    boolean batchFetchEnabled = firstBlock.length == 5;

    LinkedHashMap<Long, BlocksInfo> mapIdToBlocksInfo = new LinkedHashMap<>();
    for (String blockId : blockIds) {
      String[] blockIdParts = splitBlockId(blockId);
      if (Integer.parseInt(blockIdParts[1]) != shuffleId) {
        throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
          ", got:" + blockId);
      }
      long mapId = Long.parseLong(blockIdParts[2]);
      if (!mapIdToBlocksInfo.containsKey(mapId)) {
        mapIdToBlocksInfo.put(mapId, new BlocksInfo());
      }
      BlocksInfo blocksInfoByMapId = mapIdToBlocksInfo.get(mapId);
      blocksInfoByMapId.blockIds.add(blockId);
      blocksInfoByMapId.reduceIds.add(Integer.parseInt(blockIdParts[3]));
      if (batchFetchEnabled) {
        // When we read continuous shuffle blocks in batch, we will reuse reduceIds in
        // FetchShuffleBlocks to store the start and end reduce id for range
        // [startReduceId, endReduceId).
        assert(blockIdParts.length == 5);
        blocksInfoByMapId.reduceIds.add(Integer.parseInt(blockIdParts[4]));
      }
    }
    long[] mapIds = Longs.toArray(mapIdToBlocksInfo.keySet());
    int[][] reduceIdArr = new int[mapIds.length][];
    int blockIdIndex = 0;
    for (int i = 0; i < mapIds.length; i++) {
      BlocksInfo blocksInfoByMapId = mapIdToBlocksInfo.get(mapIds[i]);
      reduceIdArr[i] = Ints.toArray(blocksInfoByMapId.reduceIds);

      // The `blockIds`'s order must be same with the read order specified in in FetchShuffleBlocks
      // because the shuffle data's return order should match the `blockIds`'s order to ensure
      // blockId and data match.
      for (int j = 0; j < blocksInfoByMapId.blockIds.size(); j++) {
        this.blockIds[blockIdIndex++] = blocksInfoByMapId.blockIds.get(j);
      }
    }
    assert(blockIdIndex == this.blockIds.length);

    return new FetchShuffleBlocks(
      appId, execId, shuffleId, mapIds, reduceIdArr, batchFetchEnabled);
  }

  /** Split the shuffleBlockId and return shuffleId, mapId and reduceIds. */
  private String[] splitBlockId(String blockId) {
    String[] blockIdParts = blockId.split("_");
    // For batch block id, the format contains shuffleId, mapId, begin reduceId, end reduceId.
    // For single block id, the format contains shuffleId, mapId, educeId.
    if (blockIdParts.length < 4 || blockIdParts.length > 5 || !blockIdParts[0].equals("shuffle")) {
      throw new IllegalArgumentException(
        "Unexpected shuffle block id format: " + blockId);
    }
    return blockIdParts;
  }

  /** The reduceIds and blocks in a single mapId */
  private class BlocksInfo {

    final ArrayList<Integer> reduceIds;
    final ArrayList<String> blockIds;

    BlocksInfo() {
      this.reduceIds = new ArrayList<>();
      this.blockIds = new ArrayList<>();
    }
  }

  /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block. */
  private class ChunkCallback implements ChunkReceivedCallback {
    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      // On receipt of a chunk, pass it upwards as a block.
      listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
    }

    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, e);
    }
  }

  // ??????fetch??????fetch??????????????????listener???
  // ???????????????Java serializer????????????????????????RPC????????????{@link StreamHandle}???
  // ???????????????????????????????????????????????????????????????
  /**
   * Begins the fetching process, calling the listener with every block fetched.
   * The given message will be serialized with the Java serializer, and the RPC must return a
   * {@link StreamHandle}. We will send all fetch requests immediately, without throttling.
   */
  public void start() {
    client.sendRpc(message.toByteBuffer(), new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

          // ?????????????????????????????????[[ShuffleBlockFetcherIterator]]?????????????????????????????????????????????????????????????????????

          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          for (int i = 0; i < streamHandle.numChunks; i++) {
            if (downloadFileManager != null) {
              client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i),
                new DownloadCallback(i));
            } else {
              client.fetchChunk(streamHandle.streamId, i, chunkCallback);
            }
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches after success", e);
          // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
          failRemainingBlocks(blockIds, e);
        }
      }
      // ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        failRemainingBlocks(blockIds, e);
      }
    });
  }

  // ?????? listener ?????????????????? onBlockFetchFailure ??? block id
  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockFetchFailure(blockId, e);
      } catch (Exception e2) {
        logger.error("Error in block fetch failure callback", e2);
      }
    }
  }

  private class DownloadCallback implements StreamCallback {

    private DownloadFileWritableChannel channel = null;
    private DownloadFile targetFile = null;
    private int chunkIndex;

    DownloadCallback(int chunkIndex) throws IOException {
      this.targetFile = downloadFileManager.createTempFile(transportConf);
      this.channel = targetFile.openForWriting();
      this.chunkIndex = chunkIndex;
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      listener.onBlockFetchSuccess(blockIds[chunkIndex], channel.closeAndRead());
      if (!downloadFileManager.registerTempFileToClean(targetFile)) {
        targetFile.delete();
      }
    }

    @Override
    public void onFailure(String streamId, Throwable cause) throws IOException {
      channel.close();
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, cause);
      targetFile.delete();
    }
  }
}
