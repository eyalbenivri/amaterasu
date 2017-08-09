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
package org.apache.amaterasu.RunnersTests

import java.io.ByteArrayOutputStream

import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.common.execution.dependencies.{Artifact, Dependencies, Repo}
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.scalatest._

import scala.collection.mutable.ListBuffer

@DoNotDiscover
class RunnersLoadingTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  var env: Environment = _
  var factory: ProvidersFactory = _

  override protected def beforeAll(): Unit = {
    env = Environment()
    env.workingDir = "file:///tmp/"
    env.master = "local[*]"
    factory = ProvidersFactory(ExecData(env, Dependencies(ListBuffer.empty[Repo], List.empty[Artifact]), Map[String, Map[String, Any]]()), "test", new ByteArrayOutputStream(), new TestNotifier(), "test")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {

    factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner].spark.stop()

    super.afterAll()
  }


  "RunnersFactory" should "be loaded with all the implementations of AmaterasuRunner in its classpath" in {
    val r = factory.getRunner("spark", "scala")
    r should not be null
  }
}