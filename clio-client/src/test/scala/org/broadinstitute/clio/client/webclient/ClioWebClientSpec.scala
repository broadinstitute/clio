package org.broadinstitute.clio.client.webclient

import java.util.concurrent.TimeoutException

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.pattern.{after => `akka-after`}
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.util.MockClioServer
import org.broadinstitute.clio.status.model.VersionInfo
import org.broadinstitute.clio.transfer.model.{ModelMockIndex, ModelMockQueryInput}
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.Future

class ClioWebClientSpec
    extends BaseClientSpec
    with ModelAutoDerivation
    with ErrorAccumulatingCirceSupport {
  behavior of "ClioWebClient"

  val index = ModelMockIndex()
  val mockServer = new MockClioServer(index)

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.start()
  }

  override def afterAll(): Unit = {
    mockServer.stop()
    super.afterAll()
  }

  val client = new ClioWebClient(
    "localhost",
    testServerPort,
    false,
    testMaxQueued,
    testMaxConcurrent,
    testRequestTimeout,
    testMaxRetries,
    fakeTokenGenerator
  )

  it should "time out requests that take too long" in {
    recoverToSucceededIf[TimeoutException] {
      client.getClioServerHealth
    }
  }

  it should "buffer requests if too many are pushed at once" in {
    val expectedTimeout = HttpResponse(
      entity = HttpEntity(
        ContentTypes.`application/json`,
        VersionInfo("I AM THE VERSION").asJson.noSpaces
      )
    )

    val requests =
      Future.sequence(Seq.fill(testMaxQueued)(client.getClioServerVersion))
    val timeoutCheck = `akka-after`(testRequestTimeout, system.scheduler) {
      Future.successful(expectedTimeout)
    }

    for {
      firstResponse <- Future.firstCompletedOf(Seq(requests, timeoutCheck))
      _ = firstResponse should be(expectedTimeout)
      responses <- requests
      result <- responses.foldLeft(Future.successful(succeed)) {
        case (prev, response) =>
          prev.map { _ =>
            response.as[VersionInfo] should be(
              Right(VersionInfo("0.0.0-TEST"))
            )
          }
      }
    } yield {
      result
    }
  }

  it should "raise errors if too many requests are buffered at once" in {
    val requests = Seq.fill(3 * testMaxQueued)(client.getClioServerVersion)
    val sequenceAll = Future.sequence(requests)
    val sequenceMaxQueued = Future.sequence(requests.take(testMaxQueued))

    recoverToSucceededIf[RuntimeException](sequenceAll)
      .flatMap(_ => sequenceMaxQueued)
      .map(_ => succeed)
  }

  it should "retry requests that fail with connection errors" in {
    client.query(index)(ModelMockQueryInput(), includeDeleted = false).map(_ => succeed)
  }
}
