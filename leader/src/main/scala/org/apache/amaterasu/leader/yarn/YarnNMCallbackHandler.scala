/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.leader.yarn

import java.nio.ByteBuffer
import java.util

import org.apache.hadoop.yarn.api.records.{ContainerId, ContainerStatus}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync


class YarnNMCallbackHandler extends NMClientAsync.CallbackHandler {

  override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = ???

  override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit = ???

  override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = ???

  override def onContainerStarted(containerId: ContainerId, allServiceResponse: util.Map[String, ByteBuffer]): Unit = ???

  override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit = ???

  override def onContainerStopped(containerId: ContainerId): Unit = ???

}
