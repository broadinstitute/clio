package org.broadinstitute.clio.client.util

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.after
import akka.stream.ActorMaterializer
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.broadinstitute.clio.status.model.{
  ClioStatus,
  StatusInfo,
  SearchStatus,
  VersionInfo
}
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Bare-bones server to simulate parts of the clio-server needed for testing the webclient.
  */
class MockClioServer(implicit system: ActorSystem)
    extends TestData
    with ModelAutoDerivation
    with FailFastCirceSupport {
  implicit val mat: ActorMaterializer = ActorMaterializer()
  import system.dispatcher

  val schemaRequests = new AtomicInteger(0)

  /** Subset of routes exposed by the clio-server, added to as needed for testing. */
  val route: Route =
    path("health") {
      get {
        /*
         * We purposefully set up this endpoint to trigger a timeout in the client
         * so we can test that functionality.
         */
        val slowResult =
          after(testRequestTimeout + 1.second, system.scheduler) {
            Future.successful(
              StatusInfo(ClioStatus.Started, SearchStatus.OK)
            )
          }
        complete(slowResult)
      }
    } ~
      path("version") {
        get {
          /*
           * Purposefully set up this endpoint to take awhile, so we can build up a
           * bunch of requests and assert they don't all fire off at once.
           */
          val slowResult =
            after(testRequestTimeout - 1.second, system.scheduler) {
              Future.successful(VersionInfo("0.0.0-TEST"))
            }
          complete(slowResult)
        }
      } ~
      pathPrefix("api") {
        pathPrefix("v1") {
          pathPrefix(WgsUbamIndex.urlSegment) {
            path("schema") {
              /*
               * (As far as I can find) akka-http doesn't expose an API for prematurely
               * severing a connection on the server-side, so we use request timeouts as
               * a proxy for transient network problems.
               */
              val requestCount = schemaRequests.incrementAndGet()
              val schema = Future.successful(WgsUbamIndex.jsonSchema)
              val response =
                if (requestCount <= TestData.testMaxRetries) {
                  after(testRequestTimeout + 1.second, system.scheduler)(schema)
                } else {
                  schema
                }
              complete(response)
            }
          }
        }
      }

  var binding: ServerBinding = _

  def start(): Unit = {
    Http()
      .bindAndHandle(route, "0.0.0.0", testServerPort)
      .foreach { bind =>
        binding = bind
      }
  }

  def stop(): Unit = {
    if (binding != null) {
      val _ = binding.unbind()
      binding = null
    }
  }
}
