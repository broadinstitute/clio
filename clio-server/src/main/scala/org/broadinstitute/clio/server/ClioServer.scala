package org.broadinstitute.clio.server

import java.time.OffsetDateTime

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.{
  ActorMaterializer,
  ActorMaterializerSettings,
  Materializer,
  Supervision
}
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.server.service._
import org.broadinstitute.clio.server.webservice._

import scala.concurrent.ExecutionContext

object ClioServer
    extends JsonWebService
    with StatusWebService
    with AuditDirectives
    with ExceptionDirectives
    with RejectionDirectives
    with SwaggerDirectives
    with StrictLogging {

  override val serverStartTime: OffsetDateTime = OffsetDateTime.now()

  private implicit val system: ActorSystem = ActorSystem("clio")

  private implicit lazy val executionContext: ExecutionContext =
    system.dispatcher

  private val loggingDecider: Supervision.Decider = { error =>
    logger.error("stopping due to error", error)
    Supervision.Stop
  }

  private lazy val actorMaterializerSettings =
    ActorMaterializerSettings(system).withSupervisionStrategy(loggingDecider)
  private implicit lazy val materializer: Materializer = ActorMaterializer(
    actorMaterializerSettings
  )

  private val wrapperDirectives: Directive0 = {
    auditRequest & auditResult & completeWithInternalErrorJson & auditException & mapRejectionsToJson
  }
  private val infoRoutes: Route = concat(swaggerRoutes, statusRoutes)

  private val serverStatusDAO = CachedServerStatusDAO()
  private val auditDAO = LoggingAuditDAO()
  private val searchDAO = HttpElasticsearchDAO()
  private val persistenceDAO = PersistenceDAO(
    ClioServerConfig.Persistence.config,
    ClioServerConfig.Persistence.recoveryParallelism
  )

  private val app =
    new ClioApp(
      serverStatusDAO,
      auditDAO,
      persistenceDAO,
      searchDAO
    )

  private val persistenceService = PersistenceService(app)
  private val searchService = SearchService(app)
  override val auditService = AuditService(app)

  val wgsUbamWebService =
    new WgsUbamWebService(
      new WgsUbamService(persistenceService, searchService)
    )

  val gvcfWebService =
    new GvcfWebService(
      new GvcfService(persistenceService, searchService)
    )

  val wgsCramWebService =
    new WgsCramWebService(
      new WgsCramService(persistenceService, searchService)
    )

  private val apiRoutes: Route =
    concat(wgsUbamWebService.routes, gvcfWebService.routes, wgsCramWebService.routes)

  private val httpServerDAO = AkkaHttpServerDAO(wrapperDirectives, infoRoutes, apiRoutes)

  private val serverService = ServerService(app, httpServerDAO)
  override val statusService = StatusService(app, httpServerDAO)

  def beginStartup(): Unit = serverService.beginStartup()

  def awaitShutdownInf(): Unit = serverService.awaitShutdownInf()

  def shutdownAndWait(): Unit = serverService.shutdownAndWait()
}
