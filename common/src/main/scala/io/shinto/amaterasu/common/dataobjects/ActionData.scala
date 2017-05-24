package io.shinto.amaterasu.common.dataobjects

import com.google.gson.Gson
import io.shinto.amaterasu.enums.ActionStatus.ActionStatus

import scala.collection.mutable.ListBuffer

case class ActionData(var status: ActionStatus,
                      name: String,
                      src: String,
                      groupId: String,
                      typeId: String,
                      id: String,
                      nextActionIds: ListBuffer[String]) {
  var errorActionId: String = null
}

object ActionDataHelper {
  private val gson = new Gson
  def toJsonString(actionData: ActionData): String = {
    gson.toJson(actionData)
  }

  def fromJsonString(jsonString: String) : ActionData = {
    gson.fromJson[ActionData](jsonString, ActionData.getClass)
  }
}