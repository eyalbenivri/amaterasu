package org.apache.amaterasu.executor.yarn.executors

import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.logging.Logging

class YarnNotifier() extends Notifier with Logging {
  override def info(msg: String): Unit = {
    log.error(s"""-> ${msg}""")
  }

  override def success(line: String): Unit = {
    log.info(s"""SUCCESS: ${line}""")
  }

  override def error(line: String, msg: String): Unit = {
    log.error(s"""ERROR: ${line}: ${msg}""")
  }
}
