package org.broadinstitute.clio.server

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.server.dataaccess.{
  AkkaHttpServerDAO,
  CachedServerStatusDAO,
  HttpElasticsearchDAO,
  LoggingAuditDAO
}
import org.broadinstitute.clio.server.service._
import org.broadinstitute.clio.server.webservice._

object ClioServer
    extends StatusWebService
    with ReadGroupWebService
    with AuditDirectives
    with ExceptionDirectives
    with RejectionDirectives
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
    concat(statusRoutes, pathPrefix("api") { readGroupRoutes })
  private val routes = wrapperDirectives(innerRoutes)

  private val serverStatusDAO = CachedServerStatusDAO()
  private val auditDAO = LoggingAuditDAO()
  private val httpServerDAO = AkkaHttpServerDAO(routes)
  private val searchDAO = HttpElasticsearchDAO()

  private val app =
    new ClioApp(serverStatusDAO, auditDAO, httpServerDAO, searchDAO)

  private val serverService = ServerService(app)
  override val auditService = AuditService(app)
  override val statusService = StatusService(app)
  override val readGroupService = ReadGroupService(app)

  def beginStartup(): Unit = serverService.beginStartup()

  def awaitShutdownInf(): Unit = serverService.awaitShutdownInf()

  def shutdownAndWait(): Unit = serverService.shutdownAndWait()
}
