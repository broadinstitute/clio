package org.broadinstitute.clio

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.dataaccess.{AkkaHttpServerDAO, CachedServerStatusDAO, HttpElasticsearchDAO}
import org.broadinstitute.clio.service.{ServerService, StatusService}
import org.broadinstitute.clio.webservice.{ClioWebService, StatusWebService}

object ClioServer
  extends ClioWebService
    with StatusWebService
    with StrictLogging {

  override implicit val system = ActorSystem("clio")

  private implicit def executionContext = system.dispatcher

  private val loggingDecider: Supervision.Decider = {
    error =>
      logger.error("stopping due to error", error)
      Supervision.Stop
  }

  private lazy val actorMaterializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(loggingDecider)
  override implicit lazy val materializer = ActorMaterializer(actorMaterializerSettings)

  private val routes = concat(statusRoutes)

  private val serverStatusDAO = CachedServerStatusDAO()
  private val httpServerDAO = AkkaHttpServerDAO(routes)
  private val elasticsearchDAO = HttpElasticsearchDAO()

  private val app = new ClioApp(serverStatusDAO, httpServerDAO, elasticsearchDAO)

  private val serverService = ServerService(app)
  override val statusService = StatusService(app)

  def beginStartup() = serverService.beginStartup()

  def awaitShutdownInf() = serverService.awaitShutdownInf()

  def shutdownAndWait() = serverService.shutdownAndWait()
}
