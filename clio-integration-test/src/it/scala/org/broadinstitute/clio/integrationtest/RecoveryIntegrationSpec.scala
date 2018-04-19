package org.broadinstitute.clio.integrationtest

import java.net.URI

import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Sink
import better.files.File
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient.FailedResponse
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.status.model.{ClioStatus, StatusInfo, VersionInfo}
import org.broadinstitute.clio.transfer.model.arrays.{ArraysKey, ArraysMetadata}
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfKey, GvcfMetadata}
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata}
import org.broadinstitute.clio.transfer.model.wgscram.{WgsCramKey, WgsCramMetadata}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}
import org.scalatest.OptionValues

/** Tests for recovering documents on startup. Can only run reproducibly in Docker. */
class RecoveryIntegrationSpec
    extends DockerIntegrationSpec("Clio in recovery", "Recovering metadata")
    with OptionValues {

  private val documentCount = 10000
  private val location = Location.GCP

  private def randomUri(i: Int): URI = URI.create(s"gs://the-bucket/$i/$randomId")

  private def updateDoc(json: Json, pathFieldName: String): Json = {
    val oldUri = json.hcursor.get[String](pathFieldName).fold(throw _, identity)
    val updates = Seq(
      ElasticsearchIndex.UpsertIdElasticsearchName -> UpsertId.nextId().asJson,
      pathFieldName -> s"$oldUri/$randomId".asJson
    )
    json.deepMerge(Json.fromFields(updates))
  }

  private val ubamMapper =
    ElasticsearchDocumentMapper[UbamKey, UbamMetadata]
  private val initUbams = Seq.tabulate(documentCount) { i =>
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

  private val updatedUbams = initUbams.map(updateDoc(_, "ubam_path"))

  private val gvcfMapper =
    ElasticsearchDocumentMapper[GvcfKey, GvcfMetadata]
  private val initGvcfs = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val key = GvcfKey(
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i
    )
    val metadata = GvcfMetadata(
      gvcfPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
    gvcfMapper.document(key, metadata)
  }

  private val updatedGvcfs = initGvcfs.map(updateDoc(_, "gvcf_path"))

  private val cramMapper =
    ElasticsearchDocumentMapper[WgsCramKey, WgsCramMetadata]
  private val initCrams = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val key = WgsCramKey(
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i
    )
    val metadata = WgsCramMetadata(
      cramPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
    cramMapper.document(key, metadata)
  }

  private val updatedCrams = initCrams.map(updateDoc(_, "cram_path"))

  private val arraysMapper =
    ElasticsearchDocumentMapper[ArraysKey, ArraysMetadata]
  private val initArrays = Seq.tabulate(documentCount) { i =>
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

  private val updatedArrays = initArrays.map(updateDoc(_, "vcf_path"))

  override val container = new ClioDockerComposeContainer(
    File(getClass.getResource(DockerIntegrationSpec.composeFilename)),
    DockerIntegrationSpec.elasticsearchServiceName,
    Map(
      clioFullName -> DockerIntegrationSpec.clioServicePort,
      esFullName -> DockerIntegrationSpec.elasticsearchServicePort
    ),
    Map(
      ElasticsearchIndex.Ubam -> (initUbams ++ updatedUbams),
      ElasticsearchIndex.Gvcf -> (initGvcfs ++ updatedGvcfs),
      ElasticsearchIndex.WgsCram -> (initCrams ++ updatedCrams),
      ElasticsearchIndex.Arrays -> (initArrays ++ updatedArrays)
    )
  )

  private lazy val recoveryDoneFuture = clioLogLines
    .takeWhile(!_.contains(DockerIntegrationSpec.clioReadyMessage))
    .runWith(Sink.ignore)

  it should "accept health checks before recovery is complete" in {
    runDecode[StatusInfo](ClioCommand.getServerHealthName)
      .map(_.clio should be(ClioStatus.Recovering))
  }

  it should "accept version checks before recovery is complete" in {
    runDecode[VersionInfo](ClioCommand.getServerVersionName)
      .map(_.version should be(ClioBuildInfo.version))
  }

  it should "reject upserts before recovery is complete" in {
    val tmpMetadata = writeLocalTmpJson(UbamMetadata())
    recoverToSucceededIf[RuntimeException] {
      runIgnore(
        ClioCommand.addWgsUbamName,
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
      runCollectJson(ClioCommand.queryWgsCramName)
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
    ("wgs-ubam", ClioCommand.queryWgsUbamName, "lane", updatedUbams),
    ("gvcf", ClioCommand.queryGvcfName, "version", updatedGvcfs),
    ("wgs-cram", ClioCommand.queryWgsCramName, "version", updatedCrams),
    ("arrays", ClioCommand.queryArraysName, "version", updatedArrays)
  ).foreach {
    case (name, cmd, sortField, expected) =>
      it should behave like checkRecovery(name, cmd, sortField, expected)
  }

  def checkRecovery(
    indexName: String,
    queryCommand: String,
    sortField: String,
    expected: Seq[Json]
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

    it should s"recover $indexName metdata on startup" in {
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
        val sortedExpected = expected.map(mapper)

        sortedActual should contain theSameElementsInOrderAs sortedExpected
      }
    }
  }
}
