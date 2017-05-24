package io.shinto.amaterasu.leader.yarn

import java.io.{File, FileInputStream}
import java.util.Collections

import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.leader.mesos.schedulers.JobScheduler
import io.shinto.amaterasu.leader.utilities.Args
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{LocalResource, _}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by eyalbenivri on 26/04/2017.
  */
/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object JobLauncher extends App with Logging {
  var configFile: String = _
  val parser = Args.getParser
  parser.parse(args, Args()) match {

    case Some(arguments: Args) =>
      configFile = s"${arguments.home}/amaterasu.properties"
      val config = ClusterConfig(new FileInputStream(configFile))
      val resume = arguments.jobId != null

      execute(arguments, config, resume)

    case None =>
    // arguments are bad, error message will have been displayed
  }


  def setupAppMasterEnv(appMasterEnv: mutable.HashMap[String, String], conf: YarnConfiguration) = {
    for (c <- conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                  YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH:_*)) {
      // TODO: We need to understand what to replace this with
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name, c.trim)
    }
    // TODO: We need to understand what to replace this with
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name, Environment.PWD.$ + File.separator + "*")
  }

  def execute(arguments: Args, config: ClusterConfig, resume: Boolean): Unit = {
    // Create YARN Client
    val conf = new YarnConfiguration()
    val client = YarnClient.createYarnClient()
    client.init(conf)
    client.start()

    // Create application via yarnClient
    val app = client.createApplication()

    // Set up the container launch context for the application master// Set up the container launch context for the application master
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setCommands(Collections.singletonList("$JAVA_HOME/bin/java " +
      "-Xmx256M " +
      "io.shinto.amaterasu.leader.yarn.ApplicationMaster " + arguments.toCmdString()
    ))

    // Setup jars on hdfs
    val fs = FileSystem.get(conf)
    val jarPath = new Path(arguments.jarPath)
    val jarPathQualified = fs.makeQualified(jarPath)

    if (!fs.exists(jarPathQualified)) {
      fs.mkdirs(jarPathQualified)
      // TODO: copy files to HDFS
      //fs.copyFromLocalFile(false, true, )
    }

    val jarStat = fs.getFileStatus(Path.mergePaths(jarPathQualified, new Path("bin/leader-0.2.0-all.jar")))
    val appMasterJar = Records.newRecord(classOf[LocalResource])
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath))
    appMasterJar.setSize(jarStat.getLen)
    appMasterJar.setTimestamp(jarStat.getModificationTime)
    appMasterJar.setType(LocalResourceType.FILE)
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC)

    amContainer.setLocalResources(mutable.HashMap[String, LocalResource](
      "app.jar" -> appMasterJar
    ))

    // Setup CLASSPATH for ApplicationMaster
    var appMasterEnv = new mutable.HashMap[String, String]()
    setupAppMasterEnv(appMasterEnv, conf)
    amContainer.setEnvironment(appMasterEnv)


    // Set up resource type requirements for ApplicationMaster
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(256)
    capability.setVirtualCores(1)

    // Finally, set-up ApplicationSubmissionContext for the application
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName(arguments.name) // application name
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(capability)
    appContext.setQueue(config.yarnQueue) // queue


    // Submit application
    val appId = appContext.getApplicationId
    log.info("Submitting application " + appId)
    client.submitApplication(appContext)

    var appReport = client.getApplicationReport(appId)
    var appState = appReport.getYarnApplicationState
    while (appState != YarnApplicationState.FINISHED &&
      appState != YarnApplicationState.KILLED &&
      appState != YarnApplicationState.FAILED) {
      Thread.sleep(100)
      appReport = client.getApplicationReport(appId)
      appState = appReport.getYarnApplicationState
    }

    log.info(
      "Application " + appId + " finished with" +
        " state " + appState +
        " at " + appReport.getFinishTime)
    //    val frameworkBuilder = Protos.FrameworkInfo.newBuilder()
//      .setName(s"${arguments.name} - Amaterasu Job")
//      .setFailoverTimeout(config.timeout)
//      .setUser(config.user)

    // TODO: test this
//    if (resume) {
//      frameworkBuilder.setId(FrameworkID.newBuilder().setValue(arguments.jobId))
//    }

//    val framework = frameworkBuilder.build()

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

//    val driver = new MesosSchedulerDriver(scheduler, framework, masterAddress)

    log.debug(s"Connecting to master on: $masterAddress")
//    driver.run()
  }


}

