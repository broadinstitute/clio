package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import akka.stream.scaladsl.{Sink, Source}
import com.dimafeng.testcontainers.ForAllTestContainer
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.wgsubam.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.Location

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Random

trait LoadTests extends ForAllTestContainer { self: BaseIntegrationSpec =>

  val documentCount = 1000
  val location = Location.GCP

  val ubams = immutable.Seq.fill(documentCount) {
    val id = randomId
    val randInt = Random.nextInt(100)

    val key = TransferWgsUbamV1Key(
      location = location,
      flowcellBarcode = s"flowcell$id",
      lane = randInt,
      libraryName = s"library$id"
    )
    val ubamPath = Seq
      .fill(randInt)(id)
      .mkString("gs://", "/", ".unmapped.bam")

    val metadata = TransferWgsUbamV1Metadata(
      ubamPath = Some(URI.create(ubamPath)),
      ubamSize = Some(randInt.toLong),
      analysisType = Some(id),
      baitIntervals = Some(id),
      dataType = Some(id),
      individualAlias = Some(id),
      initiative = Some(id),
      lcSet = Some(id),
      libraryType = Some(id),
      machineName = Some(id),
      molecularBarcodeName = Some(id),
      molecularBarcodeSequence = Some(id),
      productFamily = Some(id),
      productName = Some(id),
      productOrderId = Some(id),
      productPartNumber = Some(id),
      project = Some(id),
      readStructure = Some(id),
      researchProjectId = Some(id),
      researchProjectName = Some(id),
      rootSampleId = Some(id),
      runName = Some(id),
      sampleAlias = Some(id),
      sampleGender = Some(id),
      sampleId = Some(id),
      sampleLsid = Some(id),
      sampleType = Some(id),
      targetIntervals = Some(id)
    )

    key -> metadata
  }

  it should "not OOM on queries returning many results" in {
    val upserts = Source(ubams)
      .mapAsync(maxQueuedRequests) {
        case (key, metadata) => {
          clioWebClient.upsert(WgsUbamIndex)(key, metadata)
        }
      }
      .runWith(Sink.ignore)

    val queries = immutable.Seq.fill(maxConcurrentRequests) { () =>
      runClientGetJsonAs[Seq[TransferWgsUbamV1QueryOutput]](
        ClioCommand.queryWgsUbamName,
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
