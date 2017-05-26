package io.shinto.amaterasu.leader.yarn

import java.io.File
import java.util.Collections

import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.leader.utilities.{Args, BaseJobLauncher}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{LocalResource, _}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by eyalbenivri on 26/04/2017.
  */
/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object YarnJobLauncher extends BaseJobLauncher {
  var configFile: String = _

  override def run(arguments: Args, config: ClusterConfig, resume: Boolean): Unit = {
    // Create YARN Client
    val conf = new YarnConfiguration()
    val client = YarnClient.createYarnClient()
    client.init(conf)
    client.start()

    // Create application via yarnClient
    val app = client.createApplication()

    // Set up the container launch context for the application master
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setCommands(Collections.singletonList("$JAVA_HOME/bin/java " +
      "io.shinto.amaterasu.leader.yarn.ApplicationMaster " + arguments.toCmdString
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
    for (c <- conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH:_*)) {
      appMasterEnv.add((Environment.CLASSPATH.name, c.trim))
    }
    appMasterEnv.add((Environment.CLASSPATH.name, Environment.PWD.$ + File.separator + "*"))
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
      log.debug(s"Application $appId with is $appState")
    }

    log.info(s"Application $appId finished with state $appState at ${appReport.getFinishTime}")
  }
}

