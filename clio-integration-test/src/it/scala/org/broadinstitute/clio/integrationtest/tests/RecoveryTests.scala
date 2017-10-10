package org.broadinstitute.clio.integrationtest.tests

import java.nio.file.Files
import java.time.OffsetDateTime
import java.util.Comparator

import akka.http.scaladsl.unmarshalling.Unmarshal
import com.dimafeng.testcontainers.ForAllTestContainer
import com.sksamuel.elastic4s.circe._
import io.circe.Encoder
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1QueryOutput
import org.broadinstitute.clio.transfer.model.wgscram.TransferWgsCramV1QueryOutput
import org.broadinstitute.clio.transfer.model.wgsubam.TransferWgsUbamV1QueryOutput
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}
import org.scalatest.{Args, Status}

import scala.util.Random

/** Tests of Clio's recovery mechanisms. */
trait RecoveryTests extends ForAllTestContainer {
  self: BaseIntegrationSpec =>

  val documentCount = 50
  val location = Location.GCP

  val storedUbams = Seq.fill(documentCount) {
    val flowcellBarcode = s"flowcell$randomId"
    val lane = Random.nextInt()
    val libraryName = s"library$randomId"
    DocumentWgsUbam(
      upsertId = UpsertId.nextId(),
      entityId = s"$flowcellBarcode.$lane.$libraryName.${location.entryName}",
      flowcellBarcode = flowcellBarcode,
      lane = lane,
      libraryName = libraryName,
      location = location,
      ubamPath = Some(s"gs://$randomId/$randomId/$randomId"),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }
  val storedGvcfs = Seq.fill(documentCount) {
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val version = Random.nextInt()
    DocumentGvcf(
      upsertId = UpsertId.nextId(),
      entityId = s"${location.entryName}.$project.$sampleAlias.$version",
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = version,
      gvcfPath = Some(s"gs://$randomId/$randomId/$randomId"),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }
  val storedWgsCrams = Seq.fill(documentCount) {
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val version = Random.nextInt()
    DocumentWgsCram(
      upsertId = UpsertId.nextId(),
      entityId = s"${location.entryName}.$project.$sampleAlias.$version",
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = version,
      cramPath = Some(s"gs://$randomId/$randomId/$randomId"),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }

  // Simulate spreading the docs over time.
  val daySpread = 10L
  val today: OffsetDateTime = OffsetDateTime.now()
  val earliest: OffsetDateTime = today.minusDays(daySpread)

  def writeDocuments[D <: ClioDocument](
    documents: Seq[D],
    index: ElasticsearchIndex[D]
  )(implicit encoder: Encoder[D]): Unit = {
    val indexable =
      indexableWithCirce[D](encoder, Elastic4sAutoDerivation.implicitEncoder)

    documents.zipWithIndex.foreach {
      case (doc, i) => {
        val dateDir = earliest
          .plusDays(i.toLong / (documentCount.toLong / daySpread))
          .format(ElasticsearchIndex.dateTimeFormatter)

        val writeDir = Files.createDirectories(
          rootPersistenceDir.resolve(s"${index.rootDir}$dateDir/")
        )

        val _ = Files.write(
          writeDir.resolve(s"${doc.persistenceFilename}"),
          indexable.json(doc).getBytes
        )
      }
    }
  }

  /**
    * Before starting any containers, we write a bunch of
    * documents to local storage, so we can check that Clio
    * recovers them properly on startup.
    */
  abstract override def run(testName: Option[String], args: Args): Status = {
    logger.warn("Cleaning local persistence directory")
    Files
      .walk(rootPersistenceDir)
      .sorted(Comparator.reverseOrder())
      .forEach(f => Files.delete(f))
    Files.createDirectories(rootPersistenceDir)

    logger.info("Writing documents to local persistence directory")
    writeDocuments(storedUbams, ElasticsearchIndex.WgsUbam)
    writeDocuments(storedGvcfs, ElasticsearchIndex.Gvcf)
    writeDocuments(storedWgsCrams, ElasticsearchIndex.WgsCram)

    super.run(testName, args)
  }

  it should "recover wgs-ubam metadata on startup" in {
    for {
      ubamQuery <- runClient(
        ClioCommand.queryWgsUbamName,
        "--location",
        location.entryName
      )
      ubams <- Unmarshal(ubamQuery).to[Seq[TransferWgsUbamV1QueryOutput]]
    } yield {
      ubams should have length documentCount.toLong
      ubams.map(_.ubamPath) should contain theSameElementsAs storedUbams.map(
        _.ubamPath
      )
    }
  }

  it should "recover gvcf metadata on startup" in {
    for {
      gvcfQuery <- runClient(
        ClioCommand.queryGvcfName,
        "--location",
        location.entryName
      )
      gvcfs <- Unmarshal(gvcfQuery).to[Seq[TransferGvcfV1QueryOutput]]
    } yield {
      gvcfs should have length documentCount.toLong
      gvcfs.map(_.gvcfPath) should contain theSameElementsAs gvcfs.map(
        _.gvcfPath
      )
    }
  }

  it should "recover wgs-cram metadata on startup" in {
    for {
      cramQuery <- runClient(
        ClioCommand.queryWgsCramName,
        "--location",
        location.entryName
      )
      crams <- Unmarshal(cramQuery).to[Seq[TransferWgsCramV1QueryOutput]]
    } yield {
      crams should have length documentCount.toLong
      crams.map(_.cramPath) should contain theSameElementsAs crams.map(
        _.cramPath
      )
    }
  }
}
