package org.apache.amaterasu.RunnersTests

import java.io.ByteArrayOutputStream

import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.common.execution.dependencies.{Artifact, Dependencies, Repo}
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.executor.execution.actions.runners.ProvidersFactory
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class RunnersLoadingTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  var env: Environment = _
  var factory: ProvidersFactory = _

  override protected def beforeAll(): Unit = {
    env = Environment()
    env.workingDir = "file:///tmp/"
    env.master = "local[*]"
    factory = ProvidersFactory(ExecData(env, Dependencies(ListBuffer.empty[Repo], List.empty[Artifact])), "test", new ByteArrayOutputStream(), new TestNotifier(), "test")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {

    factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner].sc.stop()

    super.afterAll()
  }


  "RunnersFactory" should "be loaded with all the implementations of AmaterasuRunner in its classpath" in {
    val r = factory.getRunner("spark", "scala")
    r should not be null
  }
}
