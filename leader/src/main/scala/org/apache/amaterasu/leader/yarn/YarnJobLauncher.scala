/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.leader.yarn

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.util.Collections

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.leader.utilities.{Args, BaseJobLauncher}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{LocalResource, _}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object YarnJobLauncher extends BaseJobLauncher with Logging {
  var configFile: String = _
  var fs: FileSystem = _

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
    conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"))
    conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"))
    conf.set("mapreduce.job.user.classpath.first", "true")
    val client: YarnClient = YarnClient.createYarnClient()
    client.init(conf)
    client.start()

    // Create application via yarnClient
    val app = client.createApplication()

    // Set up the container launch context for the application master
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])

    // Setup jars on hdfs
    fs = FileSystem.get(conf)
    val jarPath = new Path(config.YARN.hdfsJarsPath)
    val jarPathQualified = fs.makeQualified(jarPath)

    val commands = Collections.singletonList(
      s"""$$JAVA_HOME/bin/java org.apache.amaterasu.leader.yarn.ApplicationMaster
         | ${arguments.jobId}
         | 1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/AppMaster.stdout
         | 2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/AppMaster.stderr""".stripMargin
    )
    amContainer.setCommands(commands)

    if (!fs.exists(jarPathQualified)) {
      val home = new File(arguments.home)
      home.listFiles().foreach(f =>{
        fs.mkdirs(jarPathQualified)
        fs.copyFromLocalFile(false, true, new Path(f.getAbsolutePath), jarPathQualified)
      })

    }

    // get version of build
    val version = config.version

    // get local resources pointers that will be set on the master container env
    val leaderJarPath = s"/bin/leader-$version-all.jar"
    log.info(s"Leader Jar path is '$leaderJarPath'")
    val mergedPath = Path.mergePaths(jarPathQualified, new Path(leaderJarPath))
    log.info(mergedPath.getName)
    val leaderJar: LocalResource = setLocalResourceFromPath(mergedPath)
    val propFile: LocalResource = setLocalResourceFromPath(Path.mergePaths(jarPathQualified, new Path(s"/amaterasu.properties")))

    // set local resource on master container
    val localResources = new java.util.HashMap[String, LocalResource]
    localResources.put("leader.jar", leaderJar)
    localResources.put("amaterasu.properties", propFile)
    amContainer.setLocalResources(localResources)

    // Setup CLASSPATH for ApplicationMaster
    val appMasterEnv = new java.util.HashMap[String, String]
    for (c <- conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)) {
      appMasterEnv.put(Environment.CLASSPATH.name, c.trim)
    }


    appMasterEnv.put(Environment.CLASSPATH.name, Environment.PWD.$ + File.separator + "*")

    appMasterEnv.put("AMA_CONF_PATH", s"${config.YARN.hdfsJarsPath}/amaterasu.properties")
    appMasterEnv.put("HADOOP_USER_CLASSPATH_FIRST", "true")
    amContainer.setEnvironment(appMasterEnv)

    // Set up resource type requirements for ApplicationMaster
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(config.YARN.Master.memoryMB)
    capability.setVirtualCores(config.YARN.Master.cores)

    //TODO: verify /w YARN team
    println("===> Setting up sec")
    // Setup security tokens
//    if (UserGroupInformation.isSecurityEnabled) { // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
//      println("===> Sec enabled")
//      val credentials = new Credentials
//      val tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL)
//      if (tokenRenewer == null || tokenRenewer.length == 0) throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer")
//      // For now, only getting tokens for the default file-system.
//      val tokens = fs.addDelegationTokens(tokenRenewer, credentials)
//      if (tokens != null) for (token <- tokens) {
//        println("===> Got dt for " + fs.getUri + "; " + token)
//      }
//      val dob = new DataOutputBuffer
//      credentials.writeTokenStorageToStream(dob)
//      val fsTokens = ByteBuffer.wrap(dob.getData, 0, dob.getLength)
//      amContainer.setTokens(fsTokens)
//    }

    // Finally, set-up ApplicationSubmissionContext for the application
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName(arguments.name) // application name
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(capability)
    appContext.setQueue(config.YARN.queue) // queue
    appContext.setPriority(Priority.newInstance(1))

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

      println(s"Application $appId with is $appState user is ${appReport.getUser}")
    }

    log.info(s"Application $appId finished with state $appState at ${appReport.getFinishTime}")
  }
}