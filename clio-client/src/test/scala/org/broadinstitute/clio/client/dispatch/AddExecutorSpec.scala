package org.broadinstitute.clio.client.dispatch

import java.io.IOException
import java.net.URI

import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.AddWgsUbam
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{ArraysIndex, WgsUbamIndex}
import org.broadinstitute.clio.transfer.model.arrays.ArraysMetadata
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata}
import org.broadinstitute.clio.util.model.{Location, UpsertId}
import org.scalamock.scalatest.AsyncMockFactory

class AddExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "AddExecutor"

  import ClioWebClient.UpsertAux

  // Calling it 'key' clashes with a ScalaTest member...
  private val theKey = UbamKey(
    flowcellBarcode = "abcd",
    lane = 1,
    libraryName = "efgh",
    location = Location.GCP
  )
  private val metadata =
    UbamMetadata(project = Some("the-project"), sampleAlias = Some("the-sample"))
  private val loc = URI.create("some/fake/metadata.json")
  private val id = UpsertId.nextId()

  Seq(true, false).foreach {
    it should behave like upsertTest(_)
  }

  def upsertTest(force: Boolean): Unit = {
    it should s"read and upsert metadata with force=$force" in {
      val ioUtil = mock[IoUtil]
      (ioUtil
        .readMetadata(_: WgsUbamIndex.type)(_: URI))
        .expects(*, loc)
        .returning(Source.single(metadata))

      val webClient = mock[ClioWebClient]
      // Type annotations needed for scalamockery.
      (webClient
        .upsert(_: UpsertAux[UbamKey, UbamMetadata])(
          _: UbamKey,
          _: UbamMetadata,
          _: Boolean
        ))
        .expects(WgsUbamIndex, theKey, metadata, force)
        .returning(Source.single(id.asJson))

      val executor = new AddExecutor(AddWgsUbam(theKey, loc, force))
      executor.execute(webClient, ioUtil).runWith(Sink.head).map { json =>
        json.as[UpsertId] should be(Right(id))
      }
    }
  }

  it should "fail if the given metadata can't be read from disk" in {
    val ioUtil = mock[IoUtil]
    (ioUtil
      .readMetadata(_: WgsUbamIndex.type)(_: URI))
      .expects(*, loc)
      .returning(Source.failed(new IOException("I BROKE")))

    recoverToSucceededIf[IOException] {
      val executor = new AddExecutor(AddWgsUbam(theKey, loc, false))
      executor.execute(stub[ClioWebClient], ioUtil).runWith(Sink.ignore)
    }
  }

  it should "fail if the parsed JSON can't be decoded as the expected metadata type" in {
    val otherMetadata = ArraysMetadata(chipType = Some("the-type"))

    val ioUtil = mock[IoUtil]
    (ioUtil
      .readMetadata(_: ArraysIndex.type)(_: URI))
      .expects(*, loc)
      .returning(Source.single(otherMetadata))

    val executor = new AddExecutor(AddWgsUbam(theKey, loc, false))
    recoverToSucceededIf[Exception] {
      executor.execute(stub[ClioWebClient], ioUtil).runWith(Sink.ignore)
    }
  }

  it should "fail if the upsert to the server fails" in {
    val ioUtil = mock[IoUtil]
    (ioUtil
      .readMetadata(_: WgsUbamIndex.type)(_: URI))
      .expects(*, loc)
      .returning(Source.single(metadata))

    val webClient = mock[ClioWebClient]
    (webClient
      .upsert(_: UpsertAux[UbamKey, UbamMetadata])(
        _: UbamKey,
        _: UbamMetadata,
        _: Boolean
      ))
      .expects(WgsUbamIndex, theKey, metadata, false)
      .returning(
        Source.failed(
          ClioWebClient
            .FailedResponse(StatusCodes.InternalServerError, "I BROKE")
        )
      )

    val executor = new AddExecutor(AddWgsUbam(theKey, loc, false))
    recoverToSucceededIf[ClioWebClient.FailedResponse] {
      executor.execute(webClient, ioUtil).runWith(Sink.ignore)
    }
  }
}
