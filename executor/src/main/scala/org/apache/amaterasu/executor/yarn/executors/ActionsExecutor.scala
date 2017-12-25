package org.apache.amaterasu.executor.yarn.executors

import java.io.ByteArrayOutputStream
import java.net.URLDecoder
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.amaterasu.common.dataobjects.{ExecData, TaskData}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkContext


class ActionsExecutor {

  var master: String = _
  var sc: SparkContext = _
  var jobId: String = _
  var actionName: String = _
  var taskData: TaskData = _
  var execData: ExecData = _
  var providersFactory: ProvidersFactory = _

  def execute(): Unit = {

  }
}

// launched with args:
//s"'${jobManager.jobId}' '${config.master}' '${actionData.name}' '${URLEncoder.encode(gson.toJson(taskData), "UTF-8")}' '${URLEncoder.encode(gson.toJson(execData), "UTF-8")}' '${actionData.id}-${container.getId.getContainerId}'"
object ActionsExecutorLauncher extends App with Logging {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  log.info("Starting actions executor")
  val jobId = this.args(0)
  val master = this.args(1)
  val actionName = this.args(2)
  log.info("parsing task data")
  val taskData = mapper.readValue(URLDecoder.decode(this.args(3), "UTF-8"), classOf[TaskData])
  log.info("parsing executor data")
  val execData = mapper.readValue(URLDecoder.decode(this.args(4), "UTF-8"), classOf[ExecData])
  val taskIdAndContainerId =  this.args(5)

  val actionsExecutor: ActionsExecutor = new ActionsExecutor
  actionsExecutor.master = master
  actionsExecutor.actionName = actionName
  actionsExecutor.taskData = taskData
  actionsExecutor.execData = execData

  log.info("Setup executor")
  val baos = new ByteArrayOutputStream()
  val notifier = new YarnNotifier(new YarnConfiguration())

  log.info("Setup notifier")
  // TODO: we need an yarn container id here. In the meantime... UUID
  actionsExecutor.providersFactory = ProvidersFactory(execData, jobId, baos, notifier, taskIdAndContainerId, propFile = "./amaterasu.properties")
}
