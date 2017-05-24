package io.shinto.amaterasu.leader.utilities

/**
  * Created by eyalbenivri on 26/04/2017.
  */
case class Args(
                 repo: String = "",
                 branch: String = "master",
                 env: String = "default",
                 name: String = "amaterasu-job",
                 jobId: String = null,
                 report: String = "code",
                 home: String = "",
                 jarPath: String = ""
               ) {
  def toCmdString: String = {
    s""" --repo $repo --branch $branch --env $env --name $name --jobId $jobId --report $report --home $home"""
  }

}

object Args {
  def getParser: scopt.OptionParser[Args] = {
    new scopt.OptionParser[Args]("amaterasu job") {
      head("amaterasu job", "0.2.0") //TODO: Get the version from the build

      opt[String]('r', "repo") action { (x, c) =>
        c.copy(repo = x)
      } text "The git repo containing the job"
      opt[String]('b', "branch") action { (x, c) =>
        c.copy(branch = x)
      } text "The branch to be executed (default is master)"
      opt[String]('e', "env") action { (x, c) =>
        c.copy(env = x)
      } text "The environment to be executed (test, prod, etc. values from the default env are taken if np env specified)"
      opt[String]('n', "name") action { (x, c) =>
        c.copy(name = x)
      } text "The name of the job"
      opt[String]('i', "job-id") action { (x, c) =>
        c.copy(jobId = x)
      } text "The jobId - should be passed only when resuming a job"
      opt[String]('r', "report") action { (x, c) =>
        c.copy(report = x)
      }
      opt[String]('h', "home") action { (x, c) =>
        c.copy(home = x)
      } text "The level of reporting"
      opt[String]('j', "jarPath") action { (x, c) =>
        c.copy(jarPath = x)
      } text "The path to the executable jar"
    }
  }
}
