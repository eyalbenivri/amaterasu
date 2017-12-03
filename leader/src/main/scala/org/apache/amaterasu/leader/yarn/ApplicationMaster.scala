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

import java.io.{File, FileInputStream, InputStream}
import java.util
import java.util.Collections
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import com.google.gson.Gson
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.configuration.enums.ActionStatus.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.execution.actions.NotificationLevel.NotificationLevel
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.leader.execution.{JobLoader, JobManager}
import org.apache.amaterasu.leader.utilities.DataLoader
import org.apache.curator.framework.CuratorFramework
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import scala.collection.JavaConverters._
import scala.collection.concurrent
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse

import scala.collection.concurrent
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent._
import ExecutionContext.Implicits.global

class ApplicationMaster extends AMRMClientAsync.CallbackHandler with Logging {

  println("ApplicationMaster start")
  private var jobManager: JobManager = _
  private var client: CuratorFramework = _
  private var config: ClusterConfig = _
  private var src: String = _
  private var env: String = _
  private var branch: String = _
  private var resume: Boolean = false
  private var reportLevel: NotificationLevel = _
  private var fs: FileSystem = _
  private var awsEnv: String = ""
  private var conf: YarnConfiguration = _
  private var propPath: String = ""
  private var props: InputStream = _
  private var jarPath: Path = _
  private var jarPathQualified: Path = _
  private var version: String = ""
  private var executorPath: Path = _
  private var executorJar: LocalResource = _
  // private val command = "echo I'm running"
  // val gson:Gson = new Gson()
  private val containersIdsToTaskIds: concurrent.Map[Long, String] = new ConcurrentHashMap[Long, String].asScala
  private val completedContainersAndTaskIds: concurrent.Map[Long, String] = new ConcurrentHashMap[Long, String].asScala
  private val failedTasksCounter: concurrent.Map[String, Int] = new ConcurrentHashMap[String, Int].asScala

  // this map holds the following structure:
  // slaveId
  //  |
  //  +-> taskId, actionStatus)
  private val executionMap: concurrent.Map[String, concurrent.Map[String, ActionStatus]] = new ConcurrentHashMap[String, concurrent.Map[String, ActionStatus]].asScala
  private val lock = new ReentrantLock()


  //TODO: Eyal, verify we got everything inited here
  var nmClient: NMClientAsync = _
  var allocListener: YarnRMCallbackHandler = _
  var rmClient: AMRMClientAsync[ContainerRequest] = _
  //val rmClient: AMRMClient[ContainerRequest] = AMRMClient.createAMRMClient()

  val gson: Gson = new Gson()

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

  def execute(jobId: String): Unit = {
    println("started exe")
    Thread.sleep(10000)
    conf = new YarnConfiguration()
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)

    propPath = System.getenv("PWD") + "/amaterasu.properties"
    props = new FileInputStream(new File(propPath))


    // no need for hdfs double check (nod to Aaron Rodgers)
    // jars on HDFS should have been verified by the YARN client
    fs = FileSystem.get(conf)

    config = ClusterConfig(props)

    jarPath = new Path(config.YARN.hdfsJarsPath)
    jarPathQualified = fs.makeQualified(jarPath)
    jarPathQualified = fs.makeQualified(jarPath)

    this.version = config.version

    executorPath = Path.mergePaths(jarPathQualified, new Path(s"/dist/executor-${this.version}-all.jar"))
    executorJar = setLocalResourceFromPath(executorPath)

    println("Started execute")

    nmClient = new NMClientAsyncImpl(new YarnNMCallbackHandler())

    // Initialize clients to ResourceManager and NodeManagers
    nmClient.init(conf)
    nmClient.start()

    allocListener = new YarnRMCallbackHandler(nmClient, jobManager, env, awsEnv, config, executorJar)

    rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this)
    rmClient.init(conf)
    rmClient.start()


    // Register with ResourceManager
    val appMasterHostname = NetUtils.getHostname
    println("Registering application")

    var registrationResponse: RegisterApplicationMasterResponse = null
    //synchronized {
       rmClient.registerApplicationMaster("", 0, "")
    //}
    println("Registered application")
    Thread.sleep(10000)
//    val maxMem = registrationResponse.getMaximumResourceCapability.getMemory
//    println("Max mem capability of resources in this cluster " + maxMem)
//    val maxVCores = registrationResponse.getMaximumResourceCapability.getVirtualCores
//    println("Max vcores capability of resources in this cluster " + maxVCores)
    register(jobId)
    println(s"Created jobManager. jobManager.registeredActions.size: ${jobManager.registeredActions.size}")

    // Resource requirements for worker containers
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(Math.min(config.taskMem, 256))
    capability.setVirtualCores(1)

    var askedContainers = 0
    var completedContainers = 0
    val version = this.getClass.getPackage.getImplementationVersion

    for (i <- 0 until jobManager.registeredActions.size) {
      // Priority for worker containers - priorities are intra-application
      val priority: Priority = Records.newRecord(classOf[Priority])
      priority.setPriority(askedContainers)
      val containerAsk = new ContainerRequest(capability, null, null, priority)
      println(s"Asking for container $i")
      rmClient.addContainerRequest(containerAsk)
      println(s"request for container $i sent")
    }
    println("Finished asking for containers")
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

  override def onContainersCompleted(statuses: util.List[ContainerStatus]) = ???

  override def getProgress = ???

  override def onNodesUpdated(updatedNodes: util.List[NodeReport]) = ???

  override def onShutdownRequest() = ???


  //  override def onContainersAllocated(containers: util.List[Container]) = {
  //
  //    import scala.collection.JavaConversions._
  //    for (container <- containers) {
  //      try { // Launch container by create ContainerLaunchContext
  //        val ctx = Records.newRecord(classOf[ContainerLaunchContext])
  //        ctx.setCommands(Collections.singletonList(command + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"))
  //        System.out.println("[AM] Launching container " + container.getId)
  //        nmClient.startContainerAsync(container, ctx)
  //      } catch {
  //        case ex: Exception =>
  //          System.err.println("[AM] Error launching container " + container.getId + " " + ex)
  //      }
  //    }
  //  }

  override def onContainersAllocated(containers: util.List[Container]): Unit = {
    log.info("containers allocated")
    for (container <- containers.asScala) { // Launch container by create ContainerLaunchContext
      val containerTask = Future[String] {

        val actionData = jobManager.getNextActionData
        val taskData = DataLoader.getTaskData(actionData, env)
        val execData = DataLoader.getExecutorData(env)

        val ctx = Records.newRecord(classOf[ContainerLaunchContext])
        val command =
          s"""$awsEnv env AMA_NODE=${sys.env("AMA_NODE")}

             | env SPARK_EXECUTOR_URI=http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/dist/spark-${config.Webserver.sparkVersion}
.tgz
             | java -cp executor-0.2.0-all.jar:spark-${
            config.
              Webserver.sparkVersion
          }
/lib/*
             | -Dscala.usejavacp=true
             | -Djava.library.path=/usr/lib org.apache.amaterasu.executor.yarn.executors.ActionsExecutorLauncher
             | ${jobManager.jobId} ${config.master} ${actionData.name} ${
            gson.

              toJson(taskData)
          } ${gson.toJson(execData)}""".stripMargin
        ctx.setCommands(Collections.
          singletonList(command))

        //        ctx.setLocalResources(Map[String, LocalResource] (
        //          "executor.jar" -> executorJar
        //        ))

        nmClient.startContainerAsync(container, ctx)
        actionData.id
      }

      containerTask onComplete {
        case Failure(t) => {
          println(s"launching container failed: ${t.getMessage}")
        }

        case Success(actionDataId) => {
          containersIdsToTaskIds.put(container.getId.getContainerId, actionDataId)
          println(s"launching container succeeded: ${container.getId}")
        }
      }
    }
  }

  override def onError(e: Throwable) = ???
}

object ApplicationMaster extends App {

  val appMaster = new ApplicationMaster()
  appMaster.execute(args(0))

}
