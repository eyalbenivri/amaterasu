package io.shinto.amaterasu.leader.mesos

import java.io.FileInputStream
import java.nio.file.Paths

import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.leader.mesos.schedulers.JobScheduler
import io.shinto.amaterasu.leader.utilities.Args
import org.apache.mesos.Protos.FrameworkID
import org.apache.mesos.{MesosSchedulerDriver, Protos}

/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object JobLauncher extends App with Logging {
  val parser = Args.getParser
  parser.parse(args, Args()) match {

    case Some(arguments: Args) =>

      val config = ClusterConfig(new FileInputStream(s"${arguments.home}/amaterasu.properties"))
      val resume = arguments.jobId != null

      execute(arguments, config, resume)

    case None =>
    // arguments are bad, error message will have been displayed
  }


  def execute(arguments: Args, config: ClusterConfig, resume: Boolean): Unit = {
    val frameworkBuilder = Protos.FrameworkInfo.newBuilder()
      .setName(s"${arguments.name} - Amaterasu Job")
      .setFailoverTimeout(config.timeout)
      .setUser(config.user)

    // TODO: test this
    if (resume) {
      frameworkBuilder.setId(FrameworkID.newBuilder().setValue(arguments.jobId))
    }

    val framework = frameworkBuilder.build()

    val masterAddress = s"${config.master}:${config.masterPort}"

    val scheduler = JobScheduler(
      arguments.repo,
      arguments.branch,
      arguments.env,
      resume,
      config,
      arguments.report,
      arguments.home
    )

    val driver = new MesosSchedulerDriver(scheduler, framework, masterAddress)

    log.debug(s"Connecting to master on: $masterAddress")
    driver.run()
  }
}
