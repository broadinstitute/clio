package org.broadinstitute.clio.integrationtest

import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.TimeZone

import better.files._
import com.dimafeng.testcontainers.lifecycle.TestLifecycleAware
import com.dimafeng.testcontainers.{Container, DockerComposeContainer, ExposedService}
import org.apache.commons.io.input.{Tailer, TailerListenerAdapter}
import org.testcontainers.containers.wait.strategy.{
  AbstractWaitStrategy,
  WaitStrategy,
  WaitStrategyTarget
}

/**
  * Wrapper around [[com.dimafeng.testcontainers.DockerComposeContainer]],
  * with settings tailored for our integration tests.
  *
  * Spins up a compose cluster containing a Clio server and 2 Elasticsearch nodes.
  */
class ClioDockerComposeContainer private (
  tmpDir: File,
  readyLog: String,
  seedDocuments: Map[String, String]
) extends Container
    with TestLifecycleAware {

  /**
    * Local directory which will be mounted into the compose containers to
    * store written logs.
    */
  val logDir: File = tmpDir / "logs"

  /**
    * Local directory which will be mounted into the Clio containers to
    * store persistence records.
    */
  val persistenceDir: File = tmpDir / "persistence"

  /** Log file to mount into the Clio container. */
  val clioLog: File = logDir / "clio-server" / "clio-server.log"

  /** Log files to mount into the Elasticsearch containers. */
  private val esLogs = for {
    service <- Seq("elasticsearch1", "elasticsearch2")
    filename <- Seq("docker-cluster.log", "docker-cluster_access.log")
  } yield {
    logDir / service / filename
  }

  private val clioService =
    ClioDockerComposeContainer.clioService(new AbstractWaitStrategy {

      /**
        * Watch the Clio log, waiting for it to write its "ready" message.
        */
      override def waitUntilReady(): Unit = {

        val listener: TailerListenerAdapter = new TailerListenerAdapter {
          private var tailer: Tailer = _

          override def init(tailer: Tailer): Unit = {
            this.tailer = tailer
          }

          private val lineMarkersToPrint = Seq("INFO", "WARN", "ERROR")

          override def handle(line: String): Unit = {
            if (lineMarkersToPrint.exists(line.contains)) {
              println(line)
            }
            if (line.contains(readyLog)) {
              tailer.stop()
            }
          }
        }

        val tailer = new Tailer(clioLog.toJava, listener, 250)

        println("Waiting for Clio to log startup message...")
        tailer.run()
        println("Clio ready!")
      }
    }.withStartupTimeout(Duration.ofSeconds(90)))

  lazy val container = DockerComposeContainer(
    ClioDockerComposeContainer.extractComposeFiles(tmpDir).toJava,
    exposedServices = Seq(
      clioService,
      ClioDockerComposeContainer.esService
    ),
    /*
     * Skip "docker pull" because it'll fail if we're testing against a non-
     * pushed version of clio-server.
     */
    pull = false,
    /*
     * Testcontainers doesn't pass env vars onto the docker-compose container it
     * spins up, and we need the vars in order to fill in our compose config.
     * Using the local install gets the vars to pass through.
     */
    localCompose = true,
    /*
     * Set environment variables to ensure we're testing the right versions,
     * persisting files to local disk, and avoiding timezone drift.
     */
    env = Map(
      ClioDockerComposeContainer.clioVersionVariable -> TestkitBuildInfo.version,
      ClioDockerComposeContainer.elasticsearchVersionVariable -> TestkitBuildInfo.elasticsearchVersion,
      ClioDockerComposeContainer.logDirVariable -> logDir.toString,
      ClioDockerComposeContainer.clioLogFileVariable -> clioLog.toString,
      ClioDockerComposeContainer.persistenceDirVariable -> persistenceDir.toString,
      ClioDockerComposeContainer.timezoneVariable -> TimeZone.getDefault.getID,
      ClioDockerComposeContainer.umaskVariable -> "0000"
    )
  )

  /**
    * Make sure log files exist with global rw permissions before starting containers.
    *
    * Global rw permissions are needed so the integration tests can run on Jenkins,
    * which uses an older version of Docker that bind-mounts files into containers
    * with weird uids and gids. If permissions aren't set properly, log4j will fail
    * to initialize in the ES containers and tests will fail to start.
    */
  override def start(): Unit = {

    // Clean out the log directory so the Tailer won't exit early from examining previous logs.
    if (logDir.exists) {
      logDir.delete()
    }
    logDir.createDirectories()

    // Extract logging config to mount.
    val _ = (
      ClioDockerComposeContainer.extractClioLogback(logDir),
      ClioDockerComposeContainer.extractEsLog4j(logDir)
    )

    // Make sure target log files are writeable.
    (clioLog +: esLogs).foreach { log =>
      if (!log.parent.exists) {
        val _ = log.parent.createDirectories()
      }
      if (!log.exists) {
        val _ = log.createFile()
      }
    }

    // Clean out the persistence directory, and write any seed documents to test recovery.
    if (persistenceDir.exists) {
      persistenceDir.delete()
    }
    persistenceDir.createDirectories()
    seedDocuments.foreach {
      case (relativePath, contents) =>
        val targetFile = persistenceDir / relativePath
        targetFile.parent.createDirectories()
        targetFile.write(contents)(
          Seq(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
        )
    }

    container.start()
  }

  override def stop(): Unit =
    container.stop()

  /**
    * Get the exposed hostname for one of the exposed services running in the underlying container.
    *
    * Will throw an exception if called on a name that wasn't given as an exposed
    * service at construction.
    */
  private def getServiceHost(service: ExposedService): String =
    container.getServiceHost(service.name, service.port)

  /**
    * Get the exposed port for one of the exposed services running in the underlying container.
    *
    * Will throw an exception if called on a name that wasn't given as an exposed
    * service at construction.
    */
  private def getServicePort(service: ExposedService): Int =
    container.getServicePort(service.name, service.port)

  /** Local hostname of the Clio server running in the compose network. */
  def clioHost: String = getServiceHost(clioService)

  /** Local port of the Clio server running in the compose network. */
  def clioPort: Int = getServicePort(clioService)

  /** Local hostname of one of the Elasticsearch nodes running in the compose network. */
  def esHost: String = getServiceHost(ClioDockerComposeContainer.esService)

  /** Local port of one of the Elasticsearch nodes running in the compose network. */
  def esPort: Int = getServicePort(ClioDockerComposeContainer.esService)
}

/**
  * Names of environment variables expected to be filled in by our docker-compose files.
  * The values for these fields are mostly provided by SBT using sbt-buildinfo.
  */
object ClioDockerComposeContainer {

  /** Log message written by Clio when it's ready to receive upserts and queries. */
  private[integrationtest] val ServerReadyLog = "Server started"

  /**
    * Build a Clio container network which will mark itself as "ready" when
    * the server begins to accept upserts and queries.
    */
  def waitForReadyLog(tmpDir: File): ClioDockerComposeContainer =
    new ClioDockerComposeContainer(tmpDir, ServerReadyLog, Map.empty)

  /**
    * Build a Clio container network which will mark itself as "ready" when
    * the server begins data recovery.
    */
  def waitForRecoveryLog(
    tmpDir: File,
    seedDocuments: Map[String, String] = Map.empty
  ): ClioDockerComposeContainer =
    new ClioDockerComposeContainer(tmpDir, "Recovering metadata", seedDocuments)

  /**
    * Internal host / port info for the Clio server running in the compose network.
    * Used by Testcontainers to build an externally-accessible link to the server.
    */
  private def clioService(waitStrategy: WaitStrategy): ExposedService =
    ExposedService("clio-server_1", 8080, waitStrategy)

  /**
    * Internal host / port info for an Elasticsearch node running in the compose network.
    * Used by Testcontainers to build an externally-accessible link to the node.
    */
  private val esService: ExposedService = ExposedService(
    s"elasticsearch1_1",
    9200,
    // Readiness check on Clio will also cover readiness of the ES nodes.
    new WaitStrategy {
      override def waitUntilReady(waitStrategyTarget: WaitStrategyTarget): Unit = ()
      override def withStartupTimeout(startupTimeout: Duration): WaitStrategy = this
    }
  )

  /**
    * Extract a resource file with the given name into a temp directory.
    *
    * Needed because you can't feed an InputStream from the jar into Docker.
    */
  private[this] def copyResourceToTmp(resourceName: String, tmpDir: File): File = {
    val tmp = tmpDir.createChild(resourceName).deleteOnExit()
    for {
      out <- tmp.outputStream
      resourceContents <- Resource.my.getAsStream(resourceName).autoClosed
    } {
      resourceContents.pipeTo(out)
    }
    tmp
  }

  /**
    * The compose file describing the test environment.
    *
    * The file is assumed to be located in the resources directory of the
    * integration-test code, under the same package as this class.
    */
  private def extractComposeFiles(tmpDir: File): File = {
    Seq("clio-compose.yml", "elasticsearch-compose.yml").foreach(
      copyResourceToTmp(_, tmpDir)
    )
    copyResourceToTmp("docker-compose.yml", tmpDir)
  }

  /** Logback configuration for Clio to extract and mount into the server's container. */
  private def extractClioLogback(tmpDir: File): File =
    copyResourceToTmp("clio-logback.xml", tmpDir)

  /** Log4j configuration for ES to extract and mount into the nodes' containers. */
  private def extractEsLog4j(tmpDir: File): File =
    copyResourceToTmp("elasticsearch-log4j2.properties", tmpDir)

  /** Variable used to set the version of clio-server run during tests. */
  private val clioVersionVariable = "CLIO_DOCKER_TAG"

  /** Variable used to set the version of elasticsearch run during tests. */
  private val elasticsearchVersionVariable = "ELASTICSEARCH_DOCKER_TAG"

  /** Variable used to mount log directories into our containers. */
  private val logDirVariable = "LOG_DIR"

  /** Variable used to mount the clio-server log file into its container. */
  private val clioLogFileVariable = "CLIO_LOG_FILE"

  /**
    * Variable used to mount a local directory into the clio-server container,
    * for use as a "source of truth" when persisting metadata updates.
    */
  private val persistenceDirVariable = "LOCAL_PERSISTENCE_DIR"

  /**
    * Variable used to pass the host's default timezone ID into the containerized
    * java processes, to prevent spurious failures from timezone mismatches.
    */
  private val timezoneVariable = "TZ"

  /** Variable used to set umask for files generated by Clio in Docker. */
  private val umaskVariable = "UMASK"
}
