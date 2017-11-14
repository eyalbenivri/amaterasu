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
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import com.google.gson.Gson
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.configuration.enums.ActionStatus.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.execution.actions.NotificationLevel.NotificationLevel
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.leader.execution.{JobLoader, JobManager}
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

class ApplicationMaster() extends Logging {

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

  import org.apache.hadoop.fs.FileSystem

  val conf: YarnConfiguration = new YarnConfiguration()
  conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
  conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
  conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
  conf.set("fs.file.impl", classOf[LocalFileSystem].getName)

  val nmClient: NMClientAsync = new NMClientAsyncImpl(new YarnNMCallbackHandler())

  // no need for hdfs double check (nod to Aaron Rodgers)
  // jars on HDFS should have been verified by the YARN client
  fs = FileSystem.get(conf)

//  val file = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
  val propPath = System.getenv("PWD") + "/amaterasu.properties"
  log.info(s"log: Properties file in ${propPath}")
  println(s"Properties file in ${propPath}")
  val props: InputStream = new FileInputStream(new File(propPath))
  config = ClusterConfig(props)

  val jarPath = new Path(config.YARN.hdfsJarsPath)
  val jarPathQualified = fs.makeQualified(jarPath)
  val version = config.version
  val executorPath = Path.mergePaths(jarPathQualified, new Path(s"/dist/executor-${version}-all.jar"))
  log.info(s"log: Executor jar in ${executorPath}")
  println(s"Executor jar in ${executorPath}")
  val executorJar = setLocalResourceFromPath(executorPath)

  //TODO: Eyal, verify we got everything inited here
  val allocListener = new YarnRMCallbackHandler(nmClient, jobManager, env, awsEnv, config, executorJar)
  val rmClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)
  //val rmClient: AMRMClient[ContainerRequest] = AMRMClient.createAMRMClient()

  val gson:Gson = new Gson()

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
    // Initialize clients to ResourceManager and NodeManagers
    rmClient.init(conf)
    rmClient.start()

    nmClient.init(conf)
    nmClient.start()



    // Register with ResourceManager
    log.debug("registerApplicationMaster 0")

    val appMasterHostname = NetUtils.getHostname
    rmClient.registerApplicationMaster(appMasterHostname, -1, "")
    register(jobId)

    log.debug("registerApplicationMaster 1")

    // Resource requirements for worker containers
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(config.taskMem)
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
    }
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

object ApplicationMaster extends App {

    val appMaster = new ApplicationMaster()
    appMaster.execute(args(0))

}
