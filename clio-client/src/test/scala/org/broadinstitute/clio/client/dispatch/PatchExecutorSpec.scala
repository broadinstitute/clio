package org.broadinstitute.clio.client.dispatch

import java.net.URI
import java.time.OffsetDateTime

import akka.stream.scaladsl.{Sink, Source}
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.PatchArrays
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.client.webclient.ClioWebClient.UpsertAux
import org.broadinstitute.clio.transfer.model.{ArraysIndex, ClioIndex}
import org.broadinstitute.clio.transfer.model.arrays.{ArraysKey, ArraysMetadata}
import org.broadinstitute.clio.util.model.{Location, UpsertId}
import org.scalamock.scalatest.AsyncMockFactory

import scala.collection.immutable

class PatchExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "PatchExecutor"

  import org.broadinstitute.clio.JsonUtils.JsonOps

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
    (ioUtil
      .readMetadata(_: ArraysIndex.type)(_: URI))
      .expects(*, loc)
      .returning(Source.single(newMetadata))
    ioUtil
  }

  private def setupQueryReturn(
    webClient: ClioWebClient,
    documents: immutable.Iterable[(ArraysKey, ArraysMetadata)]
  ): Unit = {
    val _ = (
      webClient
        .query(_: ClioIndex)(
          _: Json,
          _: Boolean
        )
      )
      .expects(ArraysIndex, *, true)
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
          .upsertJson(_: UpsertAux[ArraysKey, _])(
            _: ArraysKey,
            _: Json,
            _: Boolean
          )
        )
        .expects(ArraysIndex, theKey, metadata.asJson.dropNulls, false)
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
    executor.execute(webClient, getMockIoUtil()).runWith(Sink.head).map { numPatched =>
      numPatched should be(Json.fromInt(zipped.size - alreadyHasMetadata))
    }
  }

  it should "patch documents that don't have metadata defined" in {
    val webClient = mock[ClioWebClient]

    setupQueryReturn(webClient, zipped)
    setupUpserts(webClient, keys.map(k => (k, newMetadata)))

    val executor = new PatchExecutor(PatchArrays(loc))
    executor.execute(webClient, getMockIoUtil()).runWith(Sink.head).map { numPatched =>
      numPatched should be(Json.fromInt(zipped.size))
    }
  }

  it should "patch documents that have partial metadata" in {
    val twoMetadata = newMetadata.copy(chipType = Some("a_chip_type"))
    val ioUtil = mock[IoUtil]
    (ioUtil
      .readMetadata(_: ArraysIndex.type)(_: URI))
      .expects(*, loc)
      .returning(Source.single(twoMetadata))

    val webClient = mock[ClioWebClient]

    val returnedByQuery = zipped.map { km =>
      val (key, metadata) = km
      (key, metadata.copy(chipType = Some("some_other_chip_type")))
    }

    setupQueryReturn(webClient, returnedByQuery)
    setupUpserts(webClient, keys.map(k => (k, newMetadata)))

    val executor = new PatchExecutor(PatchArrays(loc))
    executor.execute(webClient, ioUtil).runWith(Sink.head).map { numPatched =>
      numPatched should be(Json.fromInt(zipped.size))
    }
  }

  it should "not upsert anything if nothing needs to be updated" in {
    val ioUtil = mock[IoUtil]
    (ioUtil
      .readMetadata(_: ArraysIndex.type)(_: URI))
      .expects(*, loc)
      .returning(Source.single(ArraysMetadata()))

    val webClient = mock[ClioWebClient]

    setupQueryReturn(webClient, zipped)

    val executor = new PatchExecutor(PatchArrays(loc))
    executor.execute(webClient, ioUtil).runWith(Sink.head).map { numPatched =>
      numPatched should be(Json.fromInt(0))
    }
  }

  Seq(
    (
      "build queries matching unpatched documents",
      ArraysMetadata(
        chipType = Some("chip-type"),
        workflowStartDate = Some(OffsetDateTime.now())
      ),
      json"""[{"bool": {"must_not": {"exists": {"field": "chip_type"}}}},
              {"bool": {"must_not": {"exists": {"field": "workflow_start_date"}}}}]"""
    ),
    ("build an unsatisfiable query when patching nothing", ArraysMetadata(), json"[]")
  ).foreach {
    case (description, metadata, shoulds) =>
      it should behave like buildQueryTest(description, metadata, shoulds)
  }

  def buildQueryTest(
    description: String,
    metadata: ArraysMetadata,
    expectedShoulds: Json
  ): Unit = {
    it should description in {
      val query = PatchExecutor.buildQueryForUnpatched(metadata.asJson.dropNulls)

      val fullExpected =
        json"""{ "query": { "bool": { "should": $expectedShoulds, "minimum_should_match": 1 } } }"""

      query should be(fullExpected)
    }
  }
}
