/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.common.configuration

import java.io.{File, InputStream}
import java.nio.file.Paths
import java.util.Properties

import org.apache.amaterasu.common.logging.Logging

class ClusterConfig extends Logging {

  val DEFAULT_FILE = getClass().getResourceAsStream("/src/main/scripts/amaterasu.properties")
  //val DEFAULT_FILE = getClass().getResourceAsStream("/amaterasu.properties")
  var user: String = ""
  var zk: String = ""
  var master: String = "127.0.0.1"
  var masterPort: String = "5050"
  var timeout: Double = 600000
  var taskMem: Int = 128
  var distLocation: String = "local"
  var workingFolder: String = ""
  var pysparkPath: String = "spark-2.1.1-bin-hadoop2.7/bin/spark-submit"
  var Jar: String = _
  var JarName: String = _
  // the additionalClassPath is currently for testing purposes, when amaterasu is
  // not packaged, there is a need to load the spark-assembly jar
  var additionalClassPath: String = ""

  //this should be a filesystem path that is reachable by all executors (HDFS, S3, local)

  object Webserver {
    var Port: String = ""
    var Root: String = ""
    var sparkVersion: String = ""

    def load(props: Properties): Unit = {

      if (props.containsKey("webserver.port")) Webserver.Port = props.getProperty("webserver.port")
      if (props.containsKey("webserver.root")) Webserver.Root = props.getProperty("webserver.root")
      if (props.containsKey("spark.version")) Webserver.sparkVersion = props.getProperty("spark.version")
    }
  }

  object Jobs {

    var cpus: Double = 1
    var mem: Long = 512
    var repoSize: Long = 1024

    def load(props: Properties): Unit = {

      if (props.containsKey("jobs.cpu")) cpus = props.getProperty("jobs.cpu").asInstanceOf[Double]
      if (props.containsKey("jobs.mem")) mem = props.getProperty("jobs.mem").asInstanceOf[Long]
      if (props.containsKey("jobs.repoSize")) repoSize = props.getProperty("jobs.repoSize").asInstanceOf[Long]

      Tasks.load(props)
    }

    object Tasks {

      var attempts: Int = 3
      var cpus: Int = 1
      var mem: Int = 1024

      def load(props: Properties): Unit = {

        if (props.containsKey("jobs.tasks.attempts")) attempts = props.getProperty("jobs.tasks.attempts").asInstanceOf[Int]
        if (props.containsKey("jobs.tasks.cpus")) attempts = props.getProperty("jobs.tasks.cpus").asInstanceOf[Int]
        if (props.containsKey("jobs.tasks.mem")) attempts = props.getProperty("jobs.tasks.mem").asInstanceOf[Int]

      }
    }

  }

  object AWS {

    var accessKeyId: String = ""
    var secretAccessKey: String = ""
    var distBucket: String = ""
    var distFolder: String = ""

    def load(props: Properties): Unit = {

      if (props.containsKey("aws.accessKeyId")) accessKeyId = props.getProperty("aws.accessKeyId")
      if (props.containsKey("aws.secretAccessKey")) secretAccessKey = props.getProperty("aws.secretAccessKey")
      if (props.containsKey("aws.distBucket")) distBucket = props.getProperty("aws.distBucket")
      if (props.containsKey("aws.distFolder")) distFolder = props.getProperty("aws.distFolder")

    }
  }

  object local {

    var distFolder: String = new File(".").getAbsolutePath

    def load(props: Properties): Unit = {

      if (props.containsKey("local.distFolder")) distFolder = props.getProperty("local.distFolder")

    }
  }

  def load(): Unit = {
    load(DEFAULT_FILE)
  }

  def load(file: InputStream): Unit = {
    val props: Properties = new Properties()

    props.load(file)
    file.close()

    if (props.containsKey("user")) user = props.getProperty("user")
    if (props.containsKey("zk")) zk = props.getProperty("zk")
    if (props.containsKey("master")) master = props.getProperty("master")
    if (props.containsKey("masterPort")) masterPort = props.getProperty("masterPort")
    if (props.containsKey("timeout")) timeout = props.getProperty("timeout").asInstanceOf[Double]
    if (props.containsKey("workingFolder")) {
      workingFolder = props.getProperty("workingFolder")
    }
    else {
      workingFolder = s"/user/$user"
    }

    // TODO: rethink this
    Jar = this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
    JarName = Paths.get(this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getFileName.toString

    Jobs.load(props)
    Webserver.load(props)

    distLocation match {

      case "AWS"   => AWS.load(props)
      case "local" => local.load(props)
      case _       => log.error("The distribution location must be a valid file system: local, HDFS, or AWS for S3")

    }
    AWS.load(props)
  }

}

object ClusterConfig {

  def apply(file: InputStream): ClusterConfig = {

    val config = new ClusterConfig()
    config.load(file)

    config
  }

}