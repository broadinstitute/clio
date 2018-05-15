package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.stream.scaladsl.{Sink, Source}
import better.files.File
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.PatchArrays
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.client.webclient.ClioWebClient.{QueryAux, UpsertAux}
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.{
  ArraysKey,
  ArraysMetadata,
  ArraysQueryInput
}
import org.broadinstitute.clio.transfer.model.gvcf.GvcfMetadata
import org.broadinstitute.clio.util.model.{Location, UpsertId}
import org.scalamock.scalatest.AsyncMockFactory

import scala.collection.immutable

class PatchExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "PatchExecutor"

  private val documentsInClio = 0 until 2000
  private val alreadyHasMetadata = 500

  private val loc = URI.create("some/fake/metadata.json")
  private val keys = documentsInClio map { i =>
    ArraysKey(Location.GCP, Symbol(s"chipwell_barcode_$i"), i)
  }
  private val metadatas = documentsInClio map { i =>
    ArraysMetadata(
      gtcPath = Some(URI.create(s"gs://some-bucket/some_path/v$i/the_gtc.gtc"))
    )
  }

  private val zipped = keys.zip(metadatas)

  val newMetadata = ArraysMetadata(sampleAlias = Some("patched_sample_alias"))

  private def getMockIoUtil(): IoUtil = {
    val ioUtil = mock[IoUtil]
    (ioUtil.readFileData _)
      .expects(loc)
      .returning(newMetadata.asJson.pretty(defaultPrinter))
    ioUtil
  }

  private def setupQueryReturn(
    webClient: ClioWebClient,
    documents: immutable.Iterable[(ArraysKey, ArraysMetadata)]
  ): Unit = {
    val _ = (
      webClient
        .jsonFileQuery(_: QueryAux[ArraysQueryInput])(
          _: File
        )
      )
      .expects(ArraysIndex, *)
      .returning(Source(documents.map { km =>
        km._1.asJson.deepMerge(km._2.asJson)
      }))
  }

  private def setupUpserts(
    webClient: ClioWebClient,
    documents: immutable.Iterable[(ArraysKey, ArraysMetadata)]
  ): Unit = {

    documents.foreach { km =>
      val (theKey, metadata) = km
      (
        webClient
          .upsert(_: UpsertAux[ArraysKey, ArraysMetadata])(
            _: ArraysKey,
            _: ArraysMetadata,
            _: Boolean
          )
        )
        .expects(ArraysIndex, theKey, metadata, false)
        .returning(Source.single(UpsertId.nextId().asJson))
    }
  }

  it should "not patch documents that already have metadata defined" in {
    val webClient = mock[ClioWebClient]

    val documentsWithMetadata = zipped.take(alreadyHasMetadata).map { km =>
      val (key, metadata) = km
      (key, metadata.copy(sampleAlias = newMetadata.sampleAlias))
    }
    val documentsWithoutMetadata = zipped.drop(alreadyHasMetadata)

    setupQueryReturn(webClient, documentsWithMetadata ++ documentsWithoutMetadata)
    setupUpserts(webClient, documentsWithoutMetadata.map(km => (km._1, newMetadata)))

    val executor = new PatchExecutor(PatchArrays(loc))
    executor.execute(webClient, getMockIoUtil()).runWith(Sink.seq).map { _ =>
      succeed
    }
  }

  it should "patch documents that don't have metadata defined" in {
    val webClient = mock[ClioWebClient]

    setupQueryReturn(webClient, zipped)
    setupUpserts(webClient, keys.map(k => (k, newMetadata)))

    val executor = new PatchExecutor(PatchArrays(loc))
    executor.execute(webClient, getMockIoUtil()).runWith(Sink.seq).map { _ =>
      succeed
    }
  }

  it should "patch documents that have partial metadata" in {
    val twoMetadata = newMetadata.copy(chipType = Some("a_chip_type"))
    val ioUtil = mock[IoUtil]
    (ioUtil.readFileData _)
      .expects(loc)
      .returning(twoMetadata.asJson.pretty(defaultPrinter))

    val webClient = mock[ClioWebClient]

    val returnedByQuery = zipped.map { km =>
      val (key, metadata) = km
      (key, metadata.copy(chipType = Some("some_other_chip_type")))
    }

    setupQueryReturn(webClient, returnedByQuery)
    setupUpserts(webClient, keys.map(k => (k, newMetadata)))

    val executor = new PatchExecutor(PatchArrays(loc))
    executor.execute(webClient, ioUtil).runWith(Sink.seq).map { _ =>
      succeed
    }
  }

  it should "not upsert anything if nothing needs to be updated" in {
    val ioUtil = mock[IoUtil]
    (ioUtil.readFileData _)
      .expects(loc)
      .returning(ArraysMetadata().asJson.pretty(defaultPrinter))

    val webClient = mock[ClioWebClient]

    setupQueryReturn(webClient, zipped)

    val executor = new PatchExecutor(PatchArrays(loc))
    executor.execute(webClient, ioUtil).runWith(Sink.seq).map { _ =>
      succeed
    }
  }

  it should "throw an exception if the wrong type of metadata is given" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val ioUtil = mock[IoUtil]
      (ioUtil.readFileData _)
        .expects(loc)
        .returning(GvcfMetadata(contamination = Some(0.5F)).asJson.pretty(defaultPrinter))

      val webClient = mock[ClioWebClient]

      setupQueryReturn(webClient, zipped)

      val executor = new PatchExecutor(PatchArrays(loc))
      executor.execute(webClient, ioUtil).runWith(Sink.seq)
    }
  }
}
