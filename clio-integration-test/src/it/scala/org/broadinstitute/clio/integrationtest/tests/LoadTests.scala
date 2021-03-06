package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import akka.stream.scaladsl.{Sink, Source}
import com.dimafeng.testcontainers.ForAllTestContainer
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.transfer.model.UbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{UbamExtensions, UbamKey, UbamMetadata}
import org.broadinstitute.clio.util.model.{DataType, Location}

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Random

trait LoadTests extends ForAllTestContainer { self: BaseIntegrationSpec =>

  val documentCount = 1000
  val location = Location.GCP

  val ubams = immutable.Seq.fill(documentCount) {
    val id = randomId
    val symbolId = Symbol(randomId)
    val randInt = Random.nextInt(100)

    val key = UbamKey(
      location = location,
      flowcellBarcode = s"flowcell$id",
      lane = randInt,
      libraryName = s"library$id"
    )
    val ubamPath = Seq
      .fill(randInt)(id)
      .mkString("gs://", "/", UbamExtensions.UbamExtension)

    val metadata = UbamMetadata(
      ubamPath = Some(URI.create(ubamPath)),
      ubamSize = Some(randInt.toLong),
      analysisType = Some(symbolId),
      baitIntervals = Some(symbolId),
      dataType = Some(DataType.WGS),
      individualAlias = Some(id),
      initiative = Some(id),
      lcSet = Some(symbolId),
      libraryType = Some(id),
      machineName = Some(symbolId),
      molecularBarcodeName = Some(symbolId),
      molecularBarcodeSequence = Some(symbolId),
      productFamily = Some(symbolId),
      productName = Some(symbolId),
      productOrderId = Some(symbolId),
      productPartNumber = Some(symbolId),
      project = Some(id),
      readStructure = Some(symbolId),
      researchProjectId = Some(id),
      researchProjectName = Some(id),
      rootSampleId = Some(symbolId),
      runName = Some(symbolId),
      sampleAlias = Some(id),
      sampleGender = Some(symbolId),
      sampleId = Some(symbolId),
      sampleLsid = Some(symbolId),
      sampleType = Some(symbolId),
      targetIntervals = Some(symbolId)
    )

    key -> metadata
  }

  it should "not OOM on queries returning many results" in {
    val upserts = Source(ubams)
      .flatMapMerge(
        50, {
          case (key, metadata) => {
            clioWebClient.upsert(UbamIndex)(key, metadata)
          }
        }
      )
      .runWith(Sink.ignore)

    val queries = immutable.Seq.fill(10) { () =>
      runCollectJson(
        ClioCommand.queryUbamName,
        "--location",
        location.entryName
      )
    }

    lazy val checkQueries = queries.foldLeft(Future.successful(succeed)) {
      (prev, queryThunk) =>
        prev.flatMap(_ => queryThunk()).map {
          // All we care about is that the server didn't fall over.
          _ should have length documentCount.toLong
        }
    }

    for {
      _ <- upserts
      _ <- checkQueries
    } yield {
      succeed
    }
  }
}
