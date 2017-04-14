package org.broadinstitute.clio

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.{complete, get, path}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import io.circe.Printer
import org.slf4j.LoggerFactory

import scala.io.StdIn

object Main {
  private val logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    // Adapted from http://doc.akka.io/docs/akka-http/current/scala/http/introduction.html#routing-dsl-for-http-servers
    implicit val system = ActorSystem("clio")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val bindingFuture = Http().bindAndHandle(exampleRoute, interface, port)

    logger.info(s"Server v$version online at http://$interface:$port/")
    logger.info("Press RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

  lazy val config = ConfigFactory.load
  lazy val interface = config.getString("clio.http-server.interface")
  lazy val port = config.getInt("clio.http-server.port")
  lazy val version = config.getString("clio.version")

  lazy val exampleRoute =
    path("hello") {
      get {
        // Import circe support of akka-http json marshalling
        import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
        // Import circe json marshalling
        import io.circe.generic.auto._
        // Tell circe we want to override the default nospace printer with a pretty 2-space printer
        implicit val printer: Printer = Printer.spaces2

        case class ExampleResponse(message: String)

        // Return our json response
        complete(ExampleResponse(s"Say hello to Clio $version"))
      }
    }
}
