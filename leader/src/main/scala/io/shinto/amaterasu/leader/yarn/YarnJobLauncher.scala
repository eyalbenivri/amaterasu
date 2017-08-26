package io.shinto.amaterasu.leader.yarn

import java.io.File
import java.util.Collections

import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.leader.utilities.{Args, BaseJobLauncher}
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.logging.Logging
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
object YarnJobLauncher extends BaseJobLauncher with Logging {
  var configFile: String = _
  var fs:FileSystem = null

  def setLocalResourceFromPath(path: Path): LocalResource = {
    val stat = fs.getFileStatus(path)
    val fileResource = Records.newRecord(classOf[LocalResource])
    fileResource.setResource(ConverterUtils.getYarnUrlFromPath(path))
    fileResource.setSize(stat.getLen)
    fileResource.setTimestamp(stat.getModificationTime)
    fileResource.setType(LocalResourceType.FILE)
    fileResource.setVisibility(LocalResourceVisibility.PUBLIC)
    fileResource
  }

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
    fs = FileSystem.get(conf)
    val jarPath = new Path(config.YARN.hdfsJarsPath)
    val jarPathQualified = fs.makeQualified(jarPath)

    if (!fs.exists(jarPathQualified)) {
      fs.mkdirs(jarPathQualified)
      fs.copyFromLocalFile(false, true, new Path(arguments.home), jarPathQualified)
    }

    // get version of build
    val version = this.getClass.getPackage.getImplementationVersion

    // get local resources pointers that will be set on the master container env
    val leaderJar: LocalResource = setLocalResourceFromPath(Path.mergePaths(jarPathQualified, new Path(s"bin/leader-$version-all.jar")))
    val propFile: LocalResource = setLocalResourceFromPath(Path.mergePaths(jarPathQualified, new Path(s"amaterasu.propertis")))

    // set local resource on master container
    amContainer.setLocalResources(mutable.HashMap[String, LocalResource](
      "leader.jar" -> leaderJar,
      "amaterasu.properties" -> propFile
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
    capability.setMemory(config.YARN.Master.memoryMB)
    capability.setVirtualCores(config.YARN.Master.cores)

    // Finally, set-up ApplicationSubmissionContext for the application
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName(arguments.name) // application name
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(capability)
    appContext.setQueue(config.YARN.queue) // queue

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

