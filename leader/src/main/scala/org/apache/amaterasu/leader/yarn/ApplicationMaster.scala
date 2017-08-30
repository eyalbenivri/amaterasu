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

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.configuration.enums.ActionStatus.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.execution.actions.NotificationLevel.NotificationLevel
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.leader.execution.{JobLoader, JobManager}
import org.apache.amaterasu.leader.utilities.{Args, BaseJobLauncher}
import org.apache.curator.framework.CuratorFramework
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import scala.collection.JavaConverters._
import scala.collection.concurrent

class ApplicationMaster extends Logging {
  private var jobManager: JobManager = _
  private var client: CuratorFramework = _
  private var config: ClusterConfig = _
  private var src: String = _
  private var env: String = _
  private var branch: String = _
  private var resume: Boolean = false
  private var reportLevel: NotificationLevel = _
  private var fs:FileSystem = _
  private var awsEnv: String = ""

  // this map holds the following structure:
  // slaveId
  //  |
  //  +-> taskId, actionStatus)
  private val executionMap: concurrent.Map[String, concurrent.Map[String, ActionStatus]] = new ConcurrentHashMap[String, concurrent.Map[String, ActionStatus]].asScala
  private val lock = new ReentrantLock()

  val conf: YarnConfiguration = new YarnConfiguration()

  val nmClient: NMClientAsync = new NMClientAsyncImpl(new YarnNMCallbackHandler())

  // no need for hdfs double check (nod to Aaron Rodgers)
  // jars on HDFS should have been verified by the YARN client
  fs = FileSystem.get(conf)
  val jarPath = new Path(config.YARN.hdfsJarsPath)
  val jarPathQualified = fs.makeQualified(jarPath)
  val executorJar = setLocalResourceFromPath(Path.mergePaths(jarPathQualified, new Path("dist/executor-$version-all.jar")))

  //TODO: Eyal, verify we got everything inited here
  val allocListener = new YarnRMCallbackHandler(nmClient, jobManager, env, awsEnv, config, executorJar)
  val rmClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)
  //val rmClient: AMRMClient[ContainerRequest] = AMRMClient.createAMRMClient()

  //val gson:Gson = new Gson()

  def setLocalResourceFromPath(path: Path): LocalResource = {
    val stat = fs.getFileStatus(path)
    val fileResource = Records.newRecord(classOf[LocalResource])
    fileResource.setResource(ConverterUtils.getYarnUrlFromPath(path))
    fileResource.setSize(stat.getLen)
    fileResource.setTimestamp(stat.getModificationTime)
    fileResource.setType(LocalResourceType.FILE)
    fileResource.setVisibility(LocalResourceVisibility.PUBLIC)
    fileResource
  }

  def execute(args: Args, clusterConfig: ClusterConfig): Unit = {
    // Initialize clients to ResourceManager and NodeManagers
    rmClient.init(conf)
    rmClient.start()

    nmClient.init(conf)
    nmClient.start()



    // Register with ResourceManager
    log.debug("registerApplicationMaster 0")

    val appMasterHostname = NetUtils.getHostname
    rmClient.registerApplicationMaster(appMasterHostname, -1, "")
    register(args.jobId)

    log.debug("registerApplicationMaster 1")

    // Resource requirements for worker containers
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(config.taskMem)
    capability.setVirtualCores(1)

    var askedContainers = 0
    var completedContainers = 0
    val version = this.getClass.getPackage.getImplementationVersion


    while (!jobManager.outOfActions) {

      // Priority for worker containers - priorities are intra-application
      val priority: Priority = Records.newRecord(classOf[Priority])
      priority.setPriority(askedContainers)
      val containerAsk = new ContainerRequest(capability, null, null, priority)

      rmClient.addContainerRequest(containerAsk)

      //        val ctx = Records.newRecord(classOf[ContainerLaunchContext])
//        val command = s"""$awsEnv env AMA_NODE=${sys.env("AMA_NODE")}
//             | env SPARK_EXECUTOR_URI=http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/dist/spark-${config.Webserver.sparkVersion}.tgz
//             | java -cp executor-0.2.0-all.jar:spark-${config.Webserver.sparkVersion}/lib/*
//             | -Dscala.usejavacp=true
//             | -Djava.library.path=/usr/lib org.apache.amaterasu.executor.yarn.executors.ActionsExecutorLauncher
//             | ${jobManager.jobId} ${config.master} ${actionData.name} ${gson.toJson(taskData)} ${gson.toJson(execData)}""".stripMargin
//        ctx.setCommands(Collections.singletonList(command))
//
//        ctx.setLocalResources(Map[String, LocalResource] (
//          "executor.jar" -> executorJar
//         ))
//        nmClient.startContainer(container, ctx)
//        containersIdsToTaskIds.put(container.getId.getContainerId, actionData.id)
//        askedContainers += 1
//      }
    }

    // TODO: Eyal, move this to the async API
//    while(completedContainers < askedContainers) {
//      val response = rmClient.allocate(completedContainers / askedContainers)
//      for (status <- response.getCompletedContainersStatuses) {
//        completedContainers += 1
//        log.info("Completed container " + status.getContainerId)
//      }
//      Thread.sleep(100)
//    }
  }

  def register(jobId: String) = {
    if (!resume) {

      jobManager = JobLoader.loadJob(
        src,
        branch,
        jobId,
        client,
        config.Jobs.Tasks.attempts,
        new LinkedBlockingQueue[ActionData]()
      )
    }
    else {

      JobLoader.reloadJob(
        jobId,
        client,
        config.Jobs.Tasks.attempts,
        new LinkedBlockingQueue[ActionData]()
      )

    }
    jobManager.start()
  }
}

object ApplicationMaster extends BaseJobLauncher  {

  override def run(arguments: Args, config: ClusterConfig, resume: Boolean) = {
    val appMaster = new ApplicationMaster()
    appMaster.execute(arguments, config)
  }
}
