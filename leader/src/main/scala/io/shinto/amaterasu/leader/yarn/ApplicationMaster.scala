package io.shinto.amaterasu.leader.yarn

import java.io.IOException
import java.util.Collections
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import com.google.gson.Gson
import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.common.dataobjects.{ActionData, ActionDataHelper}
import io.shinto.amaterasu.common.execution.actions.NotificationLevel.NotificationLevel
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.enums.ActionStatus.ActionStatus
import io.shinto.amaterasu.leader.execution.{JobLoader, JobManager}
import io.shinto.amaterasu.leader.utilities.{Args, BaseJobLauncher, DataLoader}
import org.apache.curator.framework.CuratorFramework
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent
/**
  * Created by eyalbenivri on 26/04/2017.
  */

class ApplicationMaster extends Logging {
  private var jobManager: JobManager = null
  private var client: CuratorFramework = null
  private var config: ClusterConfig = null
  private var src: String = null
  private var env: String = null
  private var branch: String = null
  private var resume: Boolean = false
  private var reportLevel: NotificationLevel = _
  private var fs:FileSystem = null
  private var awsEnv: String = ""

  // this map holds the following structure:
  // slaveId
  //  |
  //  +-> taskId, actionStatus)
  private val executionMap: concurrent.Map[String, concurrent.Map[String, ActionStatus]] = new ConcurrentHashMap[String, concurrent.Map[String, ActionStatus]].asScala
  private val lock = new ReentrantLock()
  private val containersIdsToTaskIds: concurrent.Map[Long, String] = new ConcurrentHashMap[Long, String].asScala

  val conf: YarnConfiguration = new YarnConfiguration()
  val rmClient: AMRMClient[ContainerRequest] = AMRMClient.createAMRMClient()
  val nmClient:NMClient = null

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

  def execute(args: Args, clusterConfig: ClusterConfig): Unit = {
    // Initialize clients to ResourceManager and NodeManagers
    rmClient.init(conf)
    rmClient.start()

    nmClient.init(conf)
    nmClient.start()

    // no need for hdfs double check (nod to Aaron Rodgers)
    // jars on HDFS should have been verified by the YARN client
    fs = FileSystem.get(conf)
    val jarPath = new Path(config.YARN.hdfsJarsPath)
    val jarPathQualified = fs.makeQualified(jarPath)

    // Register with ResourceManager
    log.debug("registerApplicationMaster 0")
    rmClient.registerApplicationMaster("", 0, "")
    register(args.jobId)
    log.debug("registerApplicationMaster 1")

    // Resource requirements for worker containers
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(config.taskMem)
    capability.setVirtualCores(1)

    var askedContainers = 0
    var completedContainers = 0
    val version = this.getClass.getPackage.getImplementationVersion
    val executorJar = setLocalResourceFromPath(Path.mergePaths(jarPathQualified, new Path("dist/executor-$version-all.jar")))

    while (!jobManager.outOfActions) {

      // Priority for worker containers - priorities are intra-application
      val priority: Priority = Records.newRecord(classOf[Priority])
      priority.setPriority(askedContainers)
      val containerAsk = new ContainerRequest(capability, null, null, priority)
      rmClient.addContainerRequest(containerAsk)

      // Obtain allocated containers, launch and check for responses
      val response = rmClient.allocate(0)

      for (container <- response.getAllocatedContainers.asScala) { // Launch container by create ContainerLaunchContext
        val actionData = jobManager.getNextActionData
        val taskData = DataLoader.getTaskData(actionData, env)
        val execData = DataLoader.getExecutorData(env)

        val ctx = Records.newRecord(classOf[ContainerLaunchContext])
        val command = s"""$awsEnv env AMA_NODE=${sys.env("AMA_NODE")}
             | env SPARK_EXECUTOR_URI=http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/dist/spark-${config.Webserver.sparkVersion}.tgz
             | java -cp executor-0.2.0-all.jar:spark-${config.Webserver.sparkVersion}/lib/*
             | -Dscala.usejavacp=true
             | -Djava.library.path=/usr/lib io.shinto.amaterasu.executor.yarn.executors.ActionsExecutorLauncher
             | ${jobManager.jobId} ${config.master} ${actionData.name} ${gson.toJson(taskData)} ${gson.toJson(execData)}""".stripMargin
        ctx.setCommands(Collections.singletonList(command))

        ctx.setLocalResources(Map[String, LocalResource] (
          "executor.jar" -> executorJar
         ))
        nmClient.startContainer(container, ctx)
        containersIdsToTaskIds.put(container.getId.getContainerId, actionData.id)
        askedContainers += 1
      }
    }

    // TODO: do this async, right after the first request.
    while(completedContainers < askedContainers) {
      val response = rmClient.allocate(completedContainers / askedContainers)
      for (status <- response.getCompletedContainersStatuses) {
        completedContainers += 1
        log.info("Completed container " + status.getContainerId)
      }
      Thread.sleep(100)
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

object ApplicationMaster extends BaseJobLauncher  {

  override def run(arguments: Args, config: ClusterConfig, resume: Boolean) = {
    val appMaster = new ApplicationMaster()
    appMaster.execute(arguments, config)
  }
}
