package org.broadinstitute.clio.server

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.sksamuel.elastic4s.circe._
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.elasticsearch.Elastic4sAutoDerivation._
import org.broadinstitute.clio.server.service._
import org.broadinstitute.clio.server.webservice._

import scala.util.{Failure, Success}

object ClioServer
    extends StatusWebService
    with WgsUbamWebService
    with GvcfWebService
    with AuditDirectives
    with ExceptionDirectives
    with RejectionDirectives
    with SwaggerDirectives
    with StrictLogging {

  private implicit val system = ActorSystem("clio")

  private implicit def executionContext = system.dispatcher

  private val loggingDecider: Supervision.Decider = { error =>
    logger.error("stopping due to error", error)
    Supervision.Stop
  }

  private lazy val actorMaterializerSettings =
    ActorMaterializerSettings(system).withSupervisionStrategy(loggingDecider)
  private implicit lazy val materializer = ActorMaterializer(
    actorMaterializerSettings
  )

  private val wrapperDirectives: Directive0 = {
    auditRequest & auditResult & completeWithInternalErrorJson & auditException & mapRejectionsToJson
  }
  private val innerRoutes: Route =
    concat(swaggerRoutes, statusRoutes, pathPrefix("api") {
      concat(wgsUbamRoutes, gvcfRoutes)
    })
  private val routes = wrapperDirectives(innerRoutes)

  private val serverStatusDAO = CachedServerStatusDAO()
  private val auditDAO = LoggingAuditDAO()
  private val httpServerDAO = AkkaHttpServerDAO(routes)
  private val searchDAO = HttpElasticsearchDAO()
  private val persistenceDAO = PersistenceDAO(
    ClioServerConfig.Persistence.config
  )

  private val app =
    new ClioApp(
      serverStatusDAO,
      auditDAO,
      httpServerDAO,
      persistenceDAO,
      searchDAO
    )

  private val serverService = ServerService(app)
  private val persistenceService = PersistenceService(app)
  private val searchService = SearchService(app)
  override val auditService = AuditService(app)
  override val statusService = StatusService(app)

  override val wgsUbamService =
    new WgsUbamService(persistenceService, searchService)
  override val gvcfService =
    new GvcfService(persistenceService, searchService)

  def run(): Unit = {
    val startupAttempt = for {
      _ <- serverService.beginStartup()
      recoveredUbamCount <- persistenceService.recoverMetadata(
        ElasticsearchIndex.WgsUbam
      )
      _ = logger.info(s"Recovered $recoveredUbamCount wgs-ubams from storage")
      recoveredGvcfCount <- persistenceService.recoverMetadata(
        ElasticsearchIndex.Gvcf
      )
      _ = logger.info(s"Recovered $recoveredGvcfCount gvcfs from storage")
      _ <- serverService.completeStartup()
    } yield ()

    startupAttempt.onComplete {
      case Success(_) => {
        logger.info("Server started")
        awaitShutdownInf()
      }
      case Failure(exception) => {
        logger.error(s"Server failed to start due to $exception", exception)
        sys.exit(1)
      }
    }
  }

  def awaitShutdownInf(): Unit = serverService.awaitShutdownInf()

  def shutdownAndWait(): Unit = serverService.shutdownAndWait()
}
