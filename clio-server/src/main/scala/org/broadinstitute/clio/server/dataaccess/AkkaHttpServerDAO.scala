package org.broadinstitute.clio.server.dataaccess

import akka.actor.{ActorSystem, Terminated}
import akka.http.scaladsl._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route, RouteConcatenation}
import akka.stream.Materializer
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.model.ApiNotReadyRejection
import org.broadinstitute.clio.transfer.model.ApiConstants._

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

class AkkaHttpServerDAO private[dataaccess] (
  wrapperDirectives: Directive0,
  infoRoutes: Route,
  realApiRoutes: Route,
  interface: String,
  port: Int,
  version: String,
  shutdownTimeout: FiniteDuration
)(
  implicit system: ActorSystem,
  executionContext: ExecutionContext,
  materializer: Materializer
) extends HttpServerDAO
    with RouteConcatenation
    with StrictLogging {

  private var bindingFutureOption: Option[Future[Http.ServerBinding]] = None

  private var currentApiRoutes: Route = reject(ApiNotReadyRejection)

  private def apiRoutes: Route = pathPrefix(apiString)(currentApiRoutes)

  override def startup(): Future[Unit] = {
    val bindingFuture = Http().bindAndHandle(
      wrapperDirectives(concat(infoRoutes, apiRoutes)),
      interface,
      port
    )

    bindingFuture onComplete {
      case Success(_) =>
        logger.info(
          s"Server v{} online at http://{}:{}/",
          version,
          interface,
          Int.box(port)
        )
      case Failure(_) => system.terminate()
    }

    bindingFutureOption = Option(bindingFuture)

    bindingFuture.void
  }

  override def enableApi(): Future[Unit] = {
    currentApiRoutes = realApiRoutes
    Future.unit
  }

  override def shutdown(): Future[Unit] = {
    val terminateFuture: Future[Terminated] =
      system.whenTerminated.value match {
        case None =>
          bindingFutureOption match {
            case Some(bindingFuture) =>
              bindingFuture flatMap { binding =>
                logger.info("Server shutting down.")
                binding.unbind() // trigger unbinding from the port
              } transformWith { _ =>
                system.terminate() // and shutdown when done
              }
            case None =>
              system.terminate()
          }
        case Some(terminatedTry) => Future.fromTry(terminatedTry)
      }
    terminateFuture.void
  }

  override def awaitShutdown(): Unit = {
    Await.result(
      system.whenTerminated.void,
      shutdownTimeout
    )
  }

  override def awaitShutdownInf(): Unit = {
    Await.result(system.whenTerminated.void, Duration.Inf)
  }
}

object AkkaHttpServerDAO {

  def apply(wrapperDirectives: Directive0, infoRoutes: Route, apiRoutes: Route)(
    implicit system: ActorSystem,
    executionContext: ExecutionContext,
    materializer: Materializer
  ): HttpServerDAO = {
    new AkkaHttpServerDAO(
      wrapperDirectives,
      infoRoutes,
      apiRoutes,
      ClioServerConfig.HttpServer.interface,
      ClioServerConfig.HttpServer.port,
      ClioServerConfig.Version.value,
      ClioServerConfig.HttpServer.shutdownTimeout
    )
  }
}
