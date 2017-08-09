package org.apache.amaterasu.common.dataobjects

import org.apache.amaterasu.common.configuration.enums.ActionStatus.ActionStatus

import scala.collection.mutable.ListBuffer

case class ActionData(var status: ActionStatus,
                      name: String,
                      src: String,
                      groupId: String,
                      typeId: String,
                      id: String,
                      exports: Map[String, String],
                      nextActionIds: ListBuffer[String]) {
  var errorActionId: String = null
}
