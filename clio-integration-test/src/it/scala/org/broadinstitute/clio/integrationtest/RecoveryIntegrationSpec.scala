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
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata
}
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata
}
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata
}
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
      ElasticsearchIndex.UpsertIdElasticsearchName -> Json
        .fromString(UpsertId.nextId().id),
      pathFieldName -> Json.fromString(s"$oldUri/$randomId")
    )
    json.deepMerge(Json.fromFields(updates))
  }

  private val initUbams = Seq.tabulate(documentCount) { i =>
    val flowcellBarcode = s"flowcell$randomId"
    val libraryName = s"library$randomId"
    val keyJson = TransferUbamV1Key(
      flowcellBarcode = flowcellBarcode,
      lane = i,
      libraryName = libraryName,
      location = location
    ).asJson
    val metadataJson = TransferUbamV1Metadata(
      ubamPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    ).asJson
    val upsertIdJson =
      Map(ElasticsearchIndex.UpsertIdElasticsearchName -> UpsertId.nextId()).asJson
    val entityIdJson = Map(
      ElasticsearchIndex.EntityIdElasticsearchName -> Symbol(
        s"$flowcellBarcode.$i.$libraryName.${location.entryName}"
      )
    ).asJson
    keyJson.deepMerge(metadataJson).deepMerge(upsertIdJson).deepMerge(entityIdJson)
  }

  private val updatedUbams = initUbams.map(updateDoc(_, "ubam_path"))

  private val initGvcfs = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val keyJson = TransferGvcfV1Key(
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i
    ).asJson
    val metadataJson = TransferGvcfV1Metadata(
      gvcfPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    ).asJson
    val upsertIdJson =
      Map(ElasticsearchIndex.UpsertIdElasticsearchName -> UpsertId.nextId()).asJson
    val entityIdJson = Map(
      ElasticsearchIndex.EntityIdElasticsearchName -> Symbol(
        s"${location.entryName}.$project.$sampleAlias.$i"
      )
    ).asJson
    keyJson.deepMerge(metadataJson).deepMerge(upsertIdJson).deepMerge(entityIdJson)
  }

  private val updatedGvcfs = initGvcfs.map(updateDoc(_, "gvcf_path"))

  private val initCrams = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val keyJson = TransferWgsCramV1Key(
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i
    ).asJson
    val metadataJson = TransferWgsCramV1Metadata(
      cramPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    ).asJson
    val upsertIdJson =
      Map(ElasticsearchIndex.UpsertIdElasticsearchName -> UpsertId.nextId()).asJson
    val entityIdJson = Map(
      ElasticsearchIndex.EntityIdElasticsearchName -> Symbol(
        s"${location.entryName}.$project.$sampleAlias.$i"
      )
    ).asJson
    keyJson.deepMerge(metadataJson).deepMerge(upsertIdJson).deepMerge(entityIdJson)
  }

  private val updatedCrams = initCrams.map(updateDoc(_, "cram_path"))

  override val container = new ClioDockerComposeContainer(
    File(getClass.getResource(DockerIntegrationSpec.composeFilename)),
    DockerIntegrationSpec.elasticsearchServiceName,
    Map(
      clioFullName -> DockerIntegrationSpec.clioServicePort,
      esFullName -> DockerIntegrationSpec.elasticsearchServicePort
    ),
    Map(
      ElasticsearchIndex.WgsUbam -> (initUbams ++ updatedUbams),
      ElasticsearchIndex.Gvcf -> (initGvcfs ++ updatedGvcfs),
      ElasticsearchIndex.WgsCram -> (initCrams ++ updatedCrams)
    )
  )

  private lazy val recoveryDoneFuture = clioLogLines
    .takeWhile(!_.contains(DockerIntegrationSpec.clioReadyMessage))
    .runWith(Sink.ignore)

  it should "accept health checks before recovery is complete" in {
    runClientGetJsonAs[StatusInfo](ClioCommand.getServerHealthName)
      .map(_.clio should be(ClioStatus.Recovering))
  }

  it should "accept version checks before recovery is complete" in {
    runClientGetJsonAs[VersionInfo](ClioCommand.getServerVersionName)
      .map(_.version should be(ClioBuildInfo.version))
  }

  it should "reject upserts before recovery is complete" in {
    val tmpMetadata = writeLocalTmpJson(TransferUbamV1Metadata())
    recoverToExceptionIf[FailedResponse] {
      runClient(
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
    }.map { err =>
      err.statusCode should be(StatusCodes.ServiceUnavailable)
    }
  }

  it should "reject queries before recovery is complete" in {
    recoverToExceptionIf[FailedResponse] {
      runClient(ClioCommand.queryWgsCramName)
    }.map { err =>
      err.statusCode should be(StatusCodes.ServiceUnavailable)
    }
  }

  it should "signal via status update when recovery is complete" in {
    for {
      _ <- recoveryDoneFuture
      status <- runClientGetJsonAs[StatusInfo](ClioCommand.getServerHealthName)
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
    ("wgs-cram", ClioCommand.queryWgsCramName, "version", updatedCrams)
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
        docs <- runClient(queryCommand, "--location", location.entryName).mapTo[Json]
      } yield {
        // Helper function for filtering out values we don't care about matching.
        def mapper(json: Json): Json = {
          json.mapObject(_.filter({
            case (k, v) => !v.isNull && !keysToDrop.contains(k)
          }))
        }

        // Sort before comparing so we can do a pairwise comparison, which saves a ton of time.
        // Remove null and internal values before comparing.
        val sortedActual = docs.asArray.value.map(mapper).sorted

        // Already sorted by construction. Remove null and internal values here too.
        val sortedExpected = expected.map(mapper)
//          .map(_.mapObject(_.filter({
//            case (k, v) => !v.isNull && !keysToDrop.contains(k)
//          })))

        sortedActual should contain theSameElementsInOrderAs sortedExpected
      }
    }
  }
}
