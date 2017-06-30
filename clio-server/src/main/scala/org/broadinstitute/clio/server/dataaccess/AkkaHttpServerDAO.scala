package org.broadinstitute.clio.server.dataaccess

import akka.actor.{ActorSystem, Terminated}
import akka.http.scaladsl._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.server.ClioServerConfig

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

class AkkaHttpServerDAO private (routes: Route)(
  implicit system: ActorSystem,
  executionContext: ExecutionContext,
  fm: Materializer
) extends HttpServerDAO
    with StrictLogging {

  private var bindingFutureOption: Option[Future[Http.ServerBinding]] = None

  override def startup(): Future[Unit] = {
    val bindingFuture = Http().bindAndHandle(
      routes,
      ClioServerConfig.HttpServer.interface,
      ClioServerConfig.HttpServer.port
    )

    bindingFuture onComplete {
      case Success(_) =>
        logger.info(
          s"Server v{} online at http://{}:{}/",
          ClioServerConfig.Version.value,
          ClioServerConfig.HttpServer.interface,
          Int.box(ClioServerConfig.HttpServer.port)
        )
      case Failure(_) => system.terminate()
    }

    bindingFutureOption = Option(bindingFuture)

    bindingFuture.void
  }

  override def getVersion: Future[String] =
    Future.successful(ClioServerConfig.Version.value)

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
      ClioServerConfig.HttpServer.shutdownTimeout
    )
  }

  override def awaitShutdownInf(): Unit = {
    Await.result(system.whenTerminated.void, Duration.Inf)
  }
}

object AkkaHttpServerDAO {
  def apply(routes: Route)(implicit system: ActorSystem,
                           executionContext: ExecutionContext,
                           fm: Materializer): HttpServerDAO = {
    new AkkaHttpServerDAO(routes)
  }
}
