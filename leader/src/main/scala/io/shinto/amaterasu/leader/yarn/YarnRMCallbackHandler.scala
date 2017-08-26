package io.shinto.amaterasu.leader.yarn

import java.util

import org.apache.hadoop.yarn.api.records.{Container, ContainerStatus, NodeReport}
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync

/**
  * Created by roadan on 23/8/17.
  */
class AmaterasuYarnRMCallbackHandler extends AMRMClientAsync.CallbackHandler {

  override def onError(e: Throwable): Unit = ???

  override def onShutdownRequest(): Unit = ???

  override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = ???

  override def getProgress: Float = ???

  override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = {
  }

  override def onContainersAllocated(containers: util.List[Container]): Unit = ???

}
