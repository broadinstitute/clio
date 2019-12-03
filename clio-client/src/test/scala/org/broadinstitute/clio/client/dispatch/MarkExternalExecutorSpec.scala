package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MarkExternalArrays
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.{ArraysKey, ArraysMetadata}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}
import org.scalamock.scalatest.AsyncMockFactory

class MarkExternalExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "MarkExternalExecutor"

  // Calling it 'key' clashes with a ScalaTest member...
  private val theKey = ArraysKey(
    chipwellBarcode = Symbol("abcdefg"),
    version = 1,
    location = Location.GCP
  )
  private val metadata = ArraysMetadata(
    vcfPath = Some(URI.create("gs://the-vcf")),
    vcfIndexPath = Some(URI.create("gs://the-vcf-tbi")),
    gtcPath = Some(URI.create("gs://the-gtc")),
    paramsPath = Some(URI.create("gs://the-params")),
    documentStatus = Some(DocumentStatus.Normal)
  )
  private val theNote = "Testing testing externally hosted"
  private val id = UpsertId.nextId()

  type Aux = ClioWebClient.UpsertAux[ArraysKey, ArraysMetadata]

  it should "mark-external cloud records" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: ArraysKey, _: Boolean))
      .expects(ArraysIndex, theKey, false)
      .returning(Source.single(metadata))

    (webClient
      .upsert(_: Aux)(_: ArraysKey, _: ArraysMetadata, _: Boolean))
      .expects(
        ArraysIndex,
        theKey,
        metadata.copy(
          documentStatus = Some(DocumentStatus.External),
          notes = Some(theNote)
        ),
        true
      )
      .returning(Source.single(id.asJson))

    val executor = new MarkExternalExecutor(MarkExternalArrays(theKey, theNote))
    executor.execute(webClient, ioUtil).runWith(Sink.head).map { json =>
      json.as[UpsertId] should be(Right(id))
    }
  }
}
