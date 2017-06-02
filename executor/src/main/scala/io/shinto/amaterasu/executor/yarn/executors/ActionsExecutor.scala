package io.shinto.amaterasu.executor.yarn.executors

import java.io.ByteArrayOutputStream

import com.google.gson.Gson
import io.shinto.amaterasu.common.dataobjects.{ExecData, TaskData}
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.executor.execution.actions.runners.ProvidersFactory
import org.apache.spark.SparkContext

/**
  * Created by eyalbenivri on 31.05.17.
  */
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
  val taskData = gson.fromJson[TaskData](this.args(3), TaskData.getClass)
  val execData = gson.fromJson[ExecData](this.args(4), ExecData.getClass)

  val actionsExecutor:ActionsExecutor = new ActionsExecutor
  actionsExecutor.master = master
  actionsExecutor.actionName = actionName
  actionsExecutor.taskData = taskData
  actionsExecutor.execData = execData
  val baos = new ByteArrayOutputStream()
  actionsExecutor.providersFactory = ProvidersFactory(execData, jobId, baos, notifier, executorInfo.getExecutorId.getValue)
}
