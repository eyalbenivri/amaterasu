package org.apache.amaterasu.executor.yarn.executors

import java.io.ByteArrayOutputStream
import java.net.{URLDecoder, URLEncoder}
import java.util.UUID

import com.google.gson.Gson
import org.apache.amaterasu.common.dataobjects.{ExecData, TaskData}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.spark.SparkContext


class ActionsExecutor {
  var master: String = _
  var sc: SparkContext = _
  var jobId: String = _
  var actionName: String = _
  var taskData:TaskData = _
  var execData:ExecData = _
  var providersFactory: ProvidersFactory = _

  def execute(): Unit = {

  }
}

// launched with args:
// ${jobManager.jobId} ${config.master} ${gson.toJson(taskData)} ${gson.toJson(execData)}
object ActionsExecutorLauncher extends App with Logging {
  val gson = new Gson()
  val jobId = this.args(0)
  val master = this.args(1)
  val actionName = this.args(2)
  val taskData = gson.fromJson[TaskData](URLDecoder.decode(this.args(3), "UTF-8"), TaskData.getClass)
  val execData = gson.fromJson[ExecData](URLDecoder.decode(this.args(4), "UTF-8"), ExecData.getClass)

  val actionsExecutor:ActionsExecutor = new ActionsExecutor
  actionsExecutor.master = master
  actionsExecutor.actionName = actionName
  actionsExecutor.taskData = taskData
  actionsExecutor.execData = execData
  val baos = new ByteArrayOutputStream()
  val notifier = new YarnNotifier(null)
  // TODO: we need an yarn container id here. In the meantime... UUID
  actionsExecutor.providersFactory = ProvidersFactory(execData, jobId, baos, notifier, UUID.randomUUID().toString)
}
