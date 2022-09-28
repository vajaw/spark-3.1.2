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

import io.netty.channel.Channel;

// 客户端连接到服务器后，在TransportServer的客户端通道上执行的引导。这允许自定义客户端通道，以允许SASL身份验证等操作。
/**
 * A bootstrap which is executed on a TransportServer's client channel once a client connects
 * to the server. This allows customizing the client channel to allow for things such as SASL
 * authentication.
 */
public interface TransportServerBootstrap {
  /**
   * Customizes the channel to include new features, if needed.
   *
   * @param channel The connected channel opened by the client.
   * @param rpcHandler The RPC handler for the server.
   * @return The RPC handler to use for the channel.
   */
  RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler);
}
