package org.broadinstitute.clio.integrationtest

import java.net.URI
import java.time.OffsetDateTime

import akka.http.scaladsl.model.StatusCodes
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.Sink
import better.files.File
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient.FailedResponse
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.status.model.{ClioStatus, StatusInfo, VersionInfo}
import org.broadinstitute.clio.transfer.model.arrays.{ArraysKey, ArraysMetadata}
import org.broadinstitute.clio.transfer.model.bam.{BamKey, BamMetadata}
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfKey, GvcfMetadata}
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata}
import org.broadinstitute.clio.transfer.model.cram.{CramKey, CramMetadata}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DataType, DocumentStatus, Location, UpsertId}
import org.scalatest.OptionValues

import scala.concurrent.duration._

/** Tests for recovering documents on startup. Can only run reproducibly in Docker. */
class RecoveryIntegrationSpec extends DockerIntegrationSpec with OptionValues {

  import RecoveryIntegrationSpec._

  private def randomUri(i: Int): URI = URI.create(s"gs://the-bucket/$i/$randomId")

  private def updateDoc(json: Json, pathFieldName: String): Json = {
    val oldUri = json.hcursor.get[String](pathFieldName).fold(throw _, identity)
    val updates = Seq(
      ElasticsearchIndex.UpsertIdElasticsearchName -> UpsertId.nextId().asJson,
      pathFieldName -> s"$oldUri/$randomId".asJson
    )
    json.deepMerge(Json.fromFields(updates))
  }

  private lazy val initUbams = Seq.tabulate(documentCount) { i =>
    val flowcellBarcode = s"flowcell$randomId"
    val libraryName = s"library$randomId"
    val key = UbamKey(
      flowcellBarcode = flowcellBarcode,
      lane = i,
      libraryName = libraryName,
      location = location
    )
    val metadata = UbamMetadata(
      ubamPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
    ubamMapper.document(key, metadata)
  }
  private lazy val updatedUbams = initUbams.map(updateDoc(_, "ubam_path"))

  private lazy val initGvcfs = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val key = GvcfKey(
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i,
      dataType = DataType.WGS
    )
    val metadata = GvcfMetadata(
      gvcfPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
    if (i < (documentCount / 2)) {
      gvcfMapper
        .document(key, metadata)
        .mapObject(_.remove("data_type"))
    } else {
      gvcfMapper.document(key, metadata)
    }
  }
  private lazy val updatedGvcfs = initGvcfs.map(updateDoc(_, "gvcf_path"))

  private lazy val initCrams = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val key = CramKey(
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i,
      dataType = DataType.WGS
    )
    val metadata = CramMetadata(
      cramPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
    if (i < (documentCount / 2)) {
      cramMapper
        .document(key, metadata)
        .mapObject(_.remove("data_type"))
    } else {
      cramMapper.document(key, metadata)
    }
  }
  private lazy val updatedCrams = initCrams.map(updateDoc(_, "cram_path"))

  private lazy val initBams = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val key = BamKey(
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i,
      dataType = DataType.WGS
    )
    val metadata = BamMetadata(
      bamPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
    if (i < (documentCount / 2)) {
      bamMapper
        .document(key, metadata)
        .mapObject(_.remove("data_type"))
    } else {
      bamMapper.document(key, metadata)
    }
  }
  private lazy val updatedBams = initBams.map(updateDoc(_, "bam_path"))

  private lazy val initArrays = Seq.tabulate(documentCount) { i =>
    val key = ArraysKey(
      location = location,
      chipwellBarcode = Symbol(s"barcocde$randomId"),
      version = i
    )
    val metadata = ArraysMetadata(
      vcfPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
    arraysMapper.document(key, metadata)
  }
  private lazy val updatedArrays = initArrays.map(updateDoc(_, "vcf_path"))

  lazy val preSeededDocuments: Map[ElasticsearchIndex[_], Seq[Json]] = Map(
    ElasticsearchIndex.Ubam -> (initUbams ++ updatedUbams),
    ElasticsearchIndex.Gvcf -> (initGvcfs ++ updatedGvcfs),
    ElasticsearchIndex.Bam -> (initBams ++ updatedBams),
    ElasticsearchIndex.Cram -> (initCrams ++ updatedCrams),
    ElasticsearchIndex.Arrays -> (initArrays ++ updatedArrays)
  )

  lazy val seedPathsToContents: Map[String, String] =
    if (preSeededDocuments.nonEmpty) {
      /*
       * Simulate spreading pre-seeded documents over time.
       *
       * NOTE: The spread calculation is meant to work around a problem with case-sensitivity
       * when running this test on OS X. Our `UpsertId`s are case-sensitive, but HFS / APFS are
       * case-insensitive by default. This can cause naming collisions when generating a ton of
       * IDs at once (like in these tests). By spreading documents into bins of 26, we try to
       * avoid the possibility of two IDs differing only in the case of the last byte being dropped
       * into the same day-directory.
       */
      val daySpread = (preSeededDocuments.map(_._2.size).max / 26).toLong
      val today: OffsetDateTime = OffsetDateTime.now()
      val earliest: OffsetDateTime = today.minusDays(daySpread)

      preSeededDocuments.flatMap {
        case (index, documents) =>
          val documentCount = documents.length

          logger.info(
            s"Seeding $documentCount documents into storage for ${index.indexName}"
          )
          documents.zipWithIndex.map {
            case (json, i) =>
              val dateDir = index.persistenceDirForDatetime(
                earliest.plusDays(i.toLong / (documentCount.toLong / daySpread))
              )

              val upsertId = json.hcursor
                .get[UpsertId](
                  ElasticsearchIndex.UpsertIdElasticsearchName
                )
                .fold(throw _, identity)

              s"$dateDir/${upsertId.persistenceFilename}" -> defaultPrinter.print(json)
          }
      }
    } else {
      Map.empty
    }

  override val container: ClioDockerComposeContainer =
    ClioDockerComposeContainer.waitForRecoveryLog(
      File(IntegrationBuildInfo.tmpDir),
      seedPathsToContents
    )

  private lazy val recoveryDoneFuture = FileTailSource
    .lines(
      path = container.clioLog.path,
      maxLineSize = 1048576,
      pollingInterval = 250.millis
    )
    .takeWhile(!_.contains(ClioDockerComposeContainer.ServerReadyLog))
    .runWith(Sink.ignore)

  it should "accept health checks before recovery is complete" in {
    runDecode[StatusInfo](ClioCommand.getServerHealthName)
      .map(_.clio should be(ClioStatus.Recovering))
  }

  it should "accept version checks before recovery is complete" in {
    runDecode[VersionInfo](ClioCommand.getServerVersionName)
      .map(_.version should be(TestkitBuildInfo.version))
  }

  it should "reject upserts before recovery is complete" in {
    val tmpMetadata = writeLocalTmpJson(UbamMetadata())
    recoverToSucceededIf[RuntimeException] {
      runIgnore(
        ClioCommand.addUbamName,
        "--flowcell-barcode",
        "some-barcode",
        "--lane",
        "1",
        "--library-name",
        "some-library",
        "--location",
        Location.GCP.entryName,
        "--metadata-location",
        tmpMetadata.toString
      )
    }
  }

  it should "reject queries before recovery is complete" in {
    recoverToExceptionIf[FailedResponse] {
      runCollectJson(ClioCommand.queryCramName)
    }.map { err =>
      err.statusCode should be(StatusCodes.ServiceUnavailable)
    }
  }

  it should "signal via status update when recovery is complete" in {
    for {
      _ <- recoveryDoneFuture
      status <- runDecode[StatusInfo](ClioCommand.getServerHealthName)
    } yield {
      status.clio should be(ClioStatus.Started)
    }
  }

  private val keysToDrop =
    Set(
      ElasticsearchIndex.UpsertIdElasticsearchName,
      ElasticsearchIndex.EntityIdElasticsearchName
    )

  Seq(
    (
      "wgs-ubam",
      ClioCommand.queryUbamName,
      "lane",
      updatedUbams,
      ElasticsearchIndex.Ubam
    ),
    ("gvcf", ClioCommand.queryGvcfName, "version", updatedGvcfs, ElasticsearchIndex.Gvcf),
    (
      "Bam",
      ClioCommand.queryBamName,
      "version",
      updatedBams,
      ElasticsearchIndex.Bam
    ),
    (
      "cram",
      ClioCommand.queryCramName,
      "version",
      updatedCrams,
      ElasticsearchIndex.Cram
    ),
    (
      "arrays",
      ClioCommand.queryArraysName,
      "version",
      updatedArrays,
      ElasticsearchIndex.Arrays
    )
  ).foreach {
    case (name, cmd, sortField, expected, index) =>
      it should behave like checkRecovery(name, cmd, sortField, expected, index)
  }

  def checkRecovery(
    indexName: String,
    queryCommand: String,
    sortField: String,
    expected: Seq[Json],
    elasticsearchIndex: ElasticsearchIndex[_]
  ): Unit = {
    implicit val jsonOrdering: Ordering[Json] =
      (x, y) => {
        val maybeRes = for {
          xVal <- x.hcursor.get[Int](sortField)
          yVal <- y.hcursor.get[Int](sortField)
        } yield {
          xVal - yVal
        }

        maybeRes.fold(throw _, identity)
      }

    it should s"recover $indexName metadata on startup" in {
      for {
        _ <- recoveryDoneFuture
        docs <- runCollectJson(queryCommand, "--location", location.entryName)
      } yield {

        // Helper function for filtering out values we don't care about matching.
        def mapper(json: Json): Json = {
          json.mapObject(_.filter {
            case (k, v) => !v.isNull && !keysToDrop.contains(k)
          })
        }

        // Sort before comparing so we can do a pairwise comparison, which saves a ton of time.
        // Remove null and internal values before comparing.
        val sortedActual = docs.map(mapper).sorted

        // Already sorted by construction. Remove null and internal values here too.
        val sortedExpected: Seq[Json] = expected
          .map(mapper)
          .map(elasticsearchIndex.defaults.deepMerge)

        sortedActual should contain theSameElementsInOrderAs sortedExpected
      }
    }
  }
}

object RecoveryIntegrationSpec extends ModelAutoDerivation {
  private val documentCount = 10000
  private val location = Location.GCP

  private val ubamMapper =
    ElasticsearchDocumentMapper[UbamKey, UbamMetadata]

  private val gvcfMapper =
    ElasticsearchDocumentMapper[GvcfKey, GvcfMetadata]

  private val bamMapper =
    ElasticsearchDocumentMapper[BamKey, BamMetadata]

  private val cramMapper =
    ElasticsearchDocumentMapper[CramKey, CramMetadata]

  private val arraysMapper =
    ElasticsearchDocumentMapper[ArraysKey, ArraysMetadata]
}
