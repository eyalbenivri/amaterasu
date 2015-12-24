package io.shinto.amaterasu.mesos.schedulers

import java.util
import java.util.concurrent.{ ConcurrentHashMap, LinkedBlockingQueue, BlockingQueue }

import io.shinto.amaterasu.enums.ActionStatus
import io.shinto.amaterasu.enums.ActionStatus.ActionStatus
import io.shinto.amaterasu.{ Config, Logging }
import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.dsl.{ JobParser, GitUtil }
import io.shinto.amaterasu.execution.JobManager

import org.apache.curator.framework.{ CuratorFrameworkFactory, CuratorFramework }
import org.apache.curator.retry.ExponentialBackoffRetry

import org.apache.mesos.Protos._
import org.apache.mesos.{ Protos, SchedulerDriver, Scheduler }

import scala.collection.JavaConverters._
import scala.collection.concurrent

/**
  * The JobScheduler is a mesos implementation. It is in charge of scheduling the execution of
  * Amaterasu actions for a specific job
  */
class JobScheduler extends Scheduler with Logging {

  private var jobManager: JobManager = null
  private var client: CuratorFramework = null
  private var config: Config = null
  private var src: String = null
  private var branch: String = null

  // this map holds the following structure:
  // slaveId
  //  |
  //  |-> taskId, actionStatus)
  private val executionMap: concurrent.Map[String, concurrent.Map[String, ActionStatus]] = new ConcurrentHashMap[String, concurrent.Map[String, ActionStatus]].asScala

  private val offersToTaskIds: concurrent.Map[String, String] = new ConcurrentHashMap[String, String].asScala

  def error(driver: SchedulerDriver, message: String) {}

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {}

  def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {}

  def disconnected(driver: SchedulerDriver) {}

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {}

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus) = {

    //status.getState match {
    //case TaskState.TASK_FINISHED => JobManager.actionCompleate(status.getTaskId)

    //}

    //  status.get
    //  TaskState.

  }

  def validateOffer(offer: Offer): Boolean = {

    val resources = offer.getResourcesList.asScala

    resources.count(r => r.getName == "cpus" && r.getScalar.getValue >= config.Jobs.Tasks.cpus) > 0 &&
      resources.count(r => r.getName == "mem" && r.getScalar.getValue >= config.Jobs.Tasks.mem) > 0
  }

  def createScalarResource(name: String, value: Double): Resource = {
    Resource.newBuilder
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(value)).build()
  }

  def offerRescinded(driver: SchedulerDriver, offerId: OfferID) = {

    val actionId = offersToTaskIds.get(offerId.getValue).get
    jobManager.reQueueAction(actionId)

  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {

    for (offer <- offers.asScala) {

      log.debug(s"offer received by Amaterasu JobScheduler ${jobManager.jobId} : $offer")

      if (validateOffer(offer)) {

        log.info(s"Accepting offer, id=${offer.getId}")
        val actionData = jobManager.getNextActionData()
        val taskId = Protos.TaskID.newBuilder().setValue(actionData.id).build()

        offersToTaskIds.put(offer.getId.getValue, taskId.getValue)

        // atomically adding a record for the slave, I'm storing all the actions
        // on a slave level to efficiently handle slave loses
        executionMap.putIfAbsent(offer.getSlaveId.toString, new ConcurrentHashMap[String, ActionStatus].asScala)

        val slaveActions = executionMap.get(offer.getSlaveId.toString).get
        slaveActions.put(taskId.getValue, ActionStatus.started)

        // TODO: implement all that masos executors jazz

      }

    }

  }

  def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {

    // cloning the git repo
    GitUtil.cloneRepo(src, branch)

    // parsing the maki.yaml and creating a JobManager to
    // coordinate the workflow based on the file
    val maki = JobParser.loadMakiFile()
    jobManager = JobParser.parse(
      frameworkId.getValue,
      maki,
      new LinkedBlockingQueue[ActionData],
      client
    )

  }

  def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo) {}

}

object JobScheduler {

  def apply(src: String, branch: String, config: Config): JobScheduler = {

    val scheduler = new JobScheduler()
    scheduler.src = src
    scheduler.branch = branch

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    scheduler.client = CuratorFrameworkFactory.newClient(config.zk, retryPolicy)
    scheduler.client.start()

    scheduler.config = config

    scheduler
  }

}