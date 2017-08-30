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

import java.util
import java.util.Collections

import com.google.gson.Gson
import com.sun.crypto.provider.AESCipher.AES128_CBC_NoPadding
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.leader.execution.JobManager
import org.apache.amaterasu.leader.utilities.DataLoader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.util.Records

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}
/**
  * Created by roadan on 23/8/17.
  */
class YarnRMCallbackHandler(nmClient: NMClientAsync,
                            jobManager: JobManager,
                            env: String,
                            awsEnv: String,
                            config: ClusterConfig,
                            executorJar: LocalResource) extends AMRMClientAsync.CallbackHandler {


  val gson:Gson = new Gson()

  override def onError(e: Throwable): Unit = ???

  override def onShutdownRequest(): Unit = ???

  override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = ???

  override def getProgress: Float = ???

  override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = {
  }

  override def onContainersAllocated(containers: util.List[Container]): Unit = {

    for (container <- containers.asScala) { // Launch container by create ContainerLaunchContext
      val containerTask = Future {

        val actionData = jobManager.getNextActionData
        val taskData = DataLoader.getTaskData(actionData, env)
        val execData = DataLoader.getExecutorData(env)

        val ctx = Records.newRecord(classOf[ContainerLaunchContext])
        val command = s"""$awsEnv env AMA_NODE=${sys.env("AMA_NODE")}
                         | env SPARK_EXECUTOR_URI=http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/dist/spark-${config.Webserver.sparkVersion}.tgz
                         | java -cp executor-0.2.0-all.jar:spark-${config.Webserver.sparkVersion}/lib/*
                         | -Dscala.usejavacp=true
                         | -Djava.library.path=/usr/lib org.apache.amaterasu.executor.yarn.executors.ActionsExecutorLauncher
                         | ${jobManager.jobId} ${config.master} ${actionData.name} ${gson.toJson(taskData)} ${gson.toJson(execData)}""".stripMargin
        ctx.setCommands(Collections.singletonList(command))

        ctx.setLocalResources(Map[String, LocalResource] (
          "executor.jar" -> executorJar
        ))

        nmClient.startContainerAsync(container, ctx)
        //TODO: Eyal, please deal with this!
//        containersIdsToTaskIds.put(container.getId.getContainerId, actionData.id)
//        askedContainers += 1

      }

      containerTask onComplete {
        case Failure(t) =>
          println(s"launching container failed: ${t.getMessage}")

        case Success(ts) =>
          println(s"launching container succeeded: ${container.getId}")
      }
    }
  }
}
