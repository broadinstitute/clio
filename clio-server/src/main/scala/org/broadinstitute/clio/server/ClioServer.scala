package org.broadinstitute.clio.server

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.server.service._
import org.broadinstitute.clio.server.webservice._

object ClioServer
    extends JsonWebService
    with StatusWebService
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

  def beginStartup(): Unit = serverService.beginStartup()

  def awaitShutdownInf(): Unit = serverService.awaitShutdownInf()

  def shutdownAndWait(): Unit = serverService.shutdownAndWait()
}
