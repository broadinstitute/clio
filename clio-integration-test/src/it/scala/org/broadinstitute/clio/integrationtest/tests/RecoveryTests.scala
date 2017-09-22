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
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentGvcf,
  DocumentWgsUbam,
  Elastic4sAutoDerivation,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.util.ClioUUIDGenerator
import org.broadinstitute.clio.transfer.model.{
  TransferGvcfV1QueryOutput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}
import org.scalatest.{Args, Status}

import scala.util.Random

/** Tests of Clio's recovery mechanisms. */
trait RecoveryTests extends ForAllTestContainer {
  self: BaseIntegrationSpec =>

  val documentCount = 10
  val location = Location.GCP

  val wgsUbams = Seq.fill(documentCount) {
    val flowcellBarcode = s"flowcell$randomId"
    val lane = Random.nextInt()
    val libraryName = s"library$randomId"
    DocumentWgsUbam(
      upsertId = ClioUUIDGenerator.getUUID(),
      entityId = s"$flowcellBarcode.$lane.$libraryName.${location.entryName}",
      flowcellBarcode = flowcellBarcode,
      lane = lane,
      libraryName = libraryName,
      location = location,
      ubamPath = Some(s"gs://$randomId/$randomId/$randomId"),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }
  val gvcfs = Seq.fill(documentCount) {
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val version = Random.nextInt()
    DocumentGvcf(
      upsertId = ClioUUIDGenerator.getUUID(),
      entityId = s"${location.entryName}.$project.$sampleAlias.$version",
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = version,
      gvcfPath = Some(s"gs://$randomId/$randomId/$randomId"),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }

  /**
    * Before starting any containers, we write a bunch of
    * documents to local storage, so we can check that Clio
    * recovers them properly on startup.
    */
  abstract override def run(testName: Option[String], args: Args): Status = {

    // Simulate spreading the docs over time.
    val daySpread = 10L
    val now = OffsetDateTime.now()
    val earliest = now.minusDays(daySpread)

    val ubamIndexable = indexableWithCirce[DocumentWgsUbam](
      implicitly[Encoder[DocumentWgsUbam]],
      Elastic4sAutoDerivation.implicitEncoder
    )
    val gvcfIndexable = indexableWithCirce[DocumentGvcf](
      implicitly[Encoder[DocumentGvcf]],
      Elastic4sAutoDerivation.implicitEncoder
    )

    println("Cleaning local persistence directory")

    Files
      .walk(rootPersistenceDir)
      .sorted(Comparator.reverseOrder())
      .forEach(f => Files.delete(f))
    Files.createDirectories(rootPersistenceDir)

    println("Writing documents to local persistence directory")

    wgsUbams.zipWithIndex.foreach {
      case (doc, i) => {
        val dateDir = earliest
          .plusDays(i.toLong / (documentCount.toLong / daySpread))
          .format(ElasticsearchIndex.dateTimeFormatter)

        val writeDir = Files.createDirectories(
          rootPersistenceDir
            .resolve(s"${ElasticsearchIndex.WgsUbam.rootDir}/$dateDir")
        )

        val _ = Files.write(
          writeDir.resolve(s"${doc.upsertId}.json"),
          ubamIndexable.json(doc).getBytes
        )
      }
    }

    gvcfs.zipWithIndex.foreach {
      case (doc, i) => {
        val dateDir = earliest
          .plusDays(i.toLong / (documentCount.toLong / daySpread))
          .format(ElasticsearchIndex.dateTimeFormatter)

        val writeDir = Files.createDirectories(
          rootPersistenceDir
            .resolve(s"${ElasticsearchIndex.Gvcf.rootDir}/$dateDir")
        )

        val _ = Files.write(
          writeDir.resolve(s"${doc.upsertId}.json"),
          gvcfIndexable.json(doc).getBytes
        )
      }
    }

    super.run(testName, args)
  }

  it should "recover metadata on startup" in {
    for {
      ubamQuery <- runClient(
        ClioCommand.queryWgsUbamName,
        "--location",
        location.entryName
      )
      ubams <- Unmarshal(ubamQuery).to[Seq[TransferWgsUbamV1QueryOutput]]
      gvcfQuery <- runClient(
        ClioCommand.queryGvcfName,
        "--location",
        location.entryName
      )
      gvcfs <- Unmarshal(gvcfQuery).to[Seq[TransferGvcfV1QueryOutput]]
    } yield {
      ubams should have length documentCount.toLong
      gvcfs should have length documentCount.toLong
    }
  }
}
