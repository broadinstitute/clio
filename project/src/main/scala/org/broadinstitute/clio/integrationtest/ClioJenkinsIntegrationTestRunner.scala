package org.broadinstitute.clio.integrationtest

import com.typesafe.config.ConfigFactory
import sbt._

import scala.sys.ShutdownHookThread
import scala.util.{Failure, Success, Try}

/**
  * Runs the clio integration test via docker-compose.
  *
  * The the testClassesDirectory contains a sub-directory with a docker-compose.yml. Docker-compose is run from the
  * directory with the yml. The actual docker images to use are all passed via environment variables. The clio docker
  * image tag is generated from the version. The other docker image names/tags are read from the generated config file
  * clio-docker-images.conf.
  */
class ClioJenkinsIntegrationTestRunner(testClassesDirectory: File,
                                       clioVersion: String,
                                       log: Logger) {

  /** Runs the integration test. */
  def run(): Unit = {
    // Log directory and environment, such that one may run the compose from the command line with
    // bash$ cd DIR; ENVKEY=ENVVAL docker-compose up ...
    log.info(s"""|Starting docker-compose
                 |  directory: $dockerComposeDirectory
                 |  env: ${dockerComposeEnvironment
                  .map({ case (key, value) => s"$key=$value" })
                  .mkString(" ")}
                 |""".stripMargin)

    // Declare a lazy val, such that when it's invoked will run tryDockerComposeCleanup() once and only once
    // Pass our instance as a thunk to addShutdownHook, that will only evaluate on a shutdown
    lazy val dockerComposeCleanupInstance = dockerComposeCleanup()
    val shutdownHook = sys addShutdownHook dockerComposeCleanupInstance

    // Go try and run the docker compose test
    val triedDockerComposeTest = tryDockerComposeTest()

    // Evaluate our lazy val from above, cleaning up only once
    // While we're here, try to remove our shutdown hook
    dockerComposeCleanupInstance
    tryRemoveShutdownHook(shutdownHook)

    // With cleanup complete, now check our test exit code
    triedDockerComposeTest match {
      case Failure(exception) => throw exception
      case Success(0)         => /* ok */
      case Success(exitCode) =>
        sys.error(s"test failed with exit code $exitCode")
    }
  }

  /**
    * Reads docker configuration settings from the config.
    * Mimic of ClioConfig that reads config our docker values from the environment.
    */
  private val configDocker = {
    val configEnvironment = ConfigFactory.parseMap(System.getenv)
    val configFile =
      ConfigFactory.parseFile(
        testClassesDirectory / ClioJenkinsIntegrationTestRunner.DockerImagesConfigFileName
      )
    configEnvironment.withFallback(configFile).getConfig("clio.docker")
  }

  /** Where the docker compose integration tests should be run from. */
  private val dockerComposeDirectory = testClassesDirectory / ClioJenkinsIntegrationTestRunner.DockerComposeDirectoryName
  private val dockerComposeEnvironment = Seq(
    "CLIO_DOCKER_TAG" -> clioVersion,
    "ELASTICSEARCH_DOCKER_TAG" -> configDocker.getString("elasticsearch")
  )

  /**
    * Runs docker-compose cluster locally, including an integration test, then shuts down.
    *
    * @return The exit code of the integration test.
    */
  private def tryDockerComposeTest(): Try[Int] = Try {
    runCommandAsync(
      Seq("docker-compose", "run", "--service-ports", "clio-server"),
      dockerComposeDirectory,
      dockerComposeEnvironment,
      log.info(_)
    )
    runCommand(
      Seq("docker-compose", "run", "clio-jenkins-integration-test"),
      dockerComposeDirectory,
      dockerComposeEnvironment,
      log.info(_)
    )
  }

  /** Runs docker-compose cleanup, ignoring any errors. */
  private def dockerComposeCleanup(): Unit = {
    Try {
      val cleanupCommand =
        Seq("docker-compose", "down", "--volume", "--rmi", "local")
      runCommand(
        cleanupCommand,
        dockerComposeDirectory,
        dockerComposeEnvironment,
        log.info(_)
      )
    }
    ()
  }

  /**
    * Removes a shutdown hook.
    *
    * @param shutdownHook Hook to remove.
    * @return A boolean result of removing the hook, or a throwable describing an error.
    */
  private def tryRemoveShutdownHook(
      shutdownHook: ShutdownHookThread
  ): Try[Boolean] = Try {
    shutdownHook.remove()
  }

  /**
    * Runs a command using scala.sys.process DSL.
    *
    * @param command     The command to run.
    * @param cwd         Directory to run from.
    * @param extraEnv    Other environment variables to pass to the command.
    * @param logFunction A function implementing logging.
    * @return The exit code of the process.
    */
  private def runCommand(command: Seq[String],
                         cwd: File,
                         extraEnv: Seq[(String, String)],
                         logFunction: String => Unit): Int = {
    import scala.sys.process._
    val process = Process(command, cwd, extraEnv: _*)
    val processLogger = ProcessLogger(logFunction)
    process ! processLogger
  }

  /**
    * Runs a command using scala.sys.process DSL.
    *
    * @param command     The command to run.
    * @param cwd         Directory to run from.
    * @param extraEnv    Other environment variables to pass to the command.
    * @param logFunction A function implementing logging.
    * @return The exit code of the process.
    */
  private def runCommandAsync(command: Seq[String],
                              cwd: File,
                              extraEnv: Seq[(String, String)],
                              logFunction: String => Unit): Unit = {
    import scala.sys.process._
    val process = Process(command, cwd, extraEnv: _*)
    val processLogger = ProcessLogger(logFunction)
    process run processLogger
    ()
  }
}

object ClioJenkinsIntegrationTestRunner {
  // Paths within the testClassesDirectory
  private val DockerComposeDirectoryName = "docker-compose-jenkins"
  private val DockerImagesConfigFileName = "clio-docker-images.conf"
}
