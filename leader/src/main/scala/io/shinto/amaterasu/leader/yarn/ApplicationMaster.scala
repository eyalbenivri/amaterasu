package io.shinto.amaterasu.leader.yarn

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.locks.ReentrantLock

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.common.dataobjects.ActionData
import io.shinto.amaterasu.common.execution.actions.NotificationLevel.NotificationLevel
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.enums.ActionStatus.ActionStatus
import io.shinto.amaterasu.leader.execution.{JobLoader, JobManager}
import io.shinto.amaterasu.leader.utilities.Args
import io.shinto.amaterasu.leader.yarn.ApplicationMaster.log
import org.apache.curator.framework.CuratorFramework
import org.apache.hadoop.yarn.api.records.{Priority, Resource}
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import scala.collection.JavaConverters._
import scala.collection.concurrent
/**
  * Created by eyalbenivri on 26/04/2017.
  */

class ApplicationMaster {
  private var jobManager: JobManager = null
  private var client: CuratorFramework = null
  private var config: ClusterConfig = null
  private var src: String = null
  private var env: String = null
  private var branch: String = null
  private var resume: Boolean = false
  private var reportLevel: NotificationLevel = _

  private var awsEnv: String = ""

  // this map holds the following structure:
  // slaveId
  //  |
  //  +-> taskId, actionStatus)
  private val executionMap: concurrent.Map[String, concurrent.Map[String, ActionStatus]] = new ConcurrentHashMap[String, concurrent.Map[String, ActionStatus]].asScala
  private val lock = new ReentrantLock()
  private val containersIdsToTaskIds: concurrent.Map[String, String] = new ConcurrentHashMap[String, String].asScala

  val conf: YarnConfiguration = new YarnConfiguration()
  val rmClient: AMRMClient[Nothing] = AMRMClient.createAMRMClient()
  val nmClient:NMClient = null

  def execute(args: Args, clusterConfig: ClusterConfig): Unit = {
    // Initialize clients to ResourceManager and NodeManagers
    rmClient.init(conf)
    rmClient.start()

    nmClient.init(conf)
    nmClient.start()

    // Register with ResourceManager
    log.debug("registerApplicationMaster 0")
    rmClient.registerApplicationMaster("", 0, "")
    register(args.jobId)
    log.debug("registerApplicationMaster 1")

    // Priority for worker containers - priorities are intra-application
    val priority: Priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    // Resource requirements for worker containers
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(config.taskMem)
    capability.setVirtualCores(1)


    while(!jobManager.outOfActions) {
      val actionData = jobManager.getNextActionData

       containersIdsToTaskIds.put(containerId, actionData.id)
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

object ApplicationMaster extends App with Logging  {

  val parser = Args.getParser

  def execute(arguments: Args, config: ClusterConfig, resume: Boolean) = {
    val appMaster = new ApplicationMaster()
    appMaster.execute(arguments, config)
  }

  parser.parse(args, Args()) match {
    case Some(arguments: Args) =>
      val config = ClusterConfig()
      val resume = arguments.jobId != null
      execute(arguments, config, resume)
    case None =>
  }
}
