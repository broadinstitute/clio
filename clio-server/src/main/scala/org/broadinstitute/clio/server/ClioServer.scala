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

object ClioServer extends StrictLogging {

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

  private val serverStatusDAO = CachedServerStatusDAO()
  private val auditDAO = LoggingAuditDAO()
  private val searchDAO = HttpElasticsearchDAO()
  private val persistenceDAO = PersistenceDAO(
    ClioServerConfig.Persistence.config,
    ClioServerConfig.Persistence.recoveryParallelism
  )

  private val statusService = new StatusService(serverStatusDAO, searchDAO)

  private val exceptionDirectives = new ExceptionDirectives
  private val swaggerDirectives = new SwaggerDirectives
  private val rejectionDirectives = new RejectionDirectives(OffsetDateTime.now())
  private val auditDirectives = new AuditDirectives(AuditService(auditDAO))

  private val wrapperDirectives: Directive0 = {
    auditDirectives.auditRequest &
      auditDirectives.auditResult &
      exceptionDirectives.completeWithInternalErrorJson &
      auditDirectives.auditException &
      rejectionDirectives.mapRejectionsToJson
  }

  val statusWebService =
    new StatusWebService(statusService)

  val apiWebServices = Seq(
    new UbamWebService(
      new UbamService(persistenceDAO, searchDAO)
    ),
    new UbamWebService(
      new WgsUbamService(persistenceDAO, searchDAO)
    ),
    new GvcfWebService(
      new GvcfService(persistenceDAO, searchDAO)
    ),
    new CramWebService(
      new CramService(persistenceDAO, searchDAO)
    ),
    new ArraysWebService(
      new ArraysService(persistenceDAO, searchDAO)
    )
  )

  private val infoRoutes: Route =
    concat(swaggerDirectives.swaggerRoutes, statusWebService.statusRoutes)

  private val apiRoutes: Route =
    concat(apiWebServices.map(_.routes): _*)

  private val httpServerDAO = AkkaHttpServerDAO(wrapperDirectives, infoRoutes, apiRoutes)

  private val serverService = new ServerService(
    serverStatusDAO,
    persistenceDAO,
    searchDAO,
    httpServerDAO,
    ClioServerConfig.Version.value
  )

  def beginStartup(): Unit = serverService.beginStartup()

  def awaitShutdownInf(): Unit = serverService.awaitShutdownInf()

  def shutdownAndWait(): Unit = serverService.shutdownAndWait()
}
