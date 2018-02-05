package org.broadinstitute.clio.integrationtest

import java.io.File
import java.net.URI

import akka.Done
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Sink
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient.FailedResponse
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentGvcf,
  DocumentWgsCram,
  DocumentWgsUbam,
  ElasticsearchIndex
}
import org.broadinstitute.clio.status.model.{ClioStatus, StatusInfo, VersionInfo}
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1QueryOutput
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Metadata,
  TransferUbamV1QueryOutput
}
import org.broadinstitute.clio.transfer.model.wgscram.TransferWgsCramV1QueryOutput
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}

import scala.concurrent.Future
import scala.util.Random

/** Tests for recovering documents on startup. Can only run reproducibly in Docker. */
class RecoveryIntegrationSpec
    extends DockerIntegrationSpec("Clio in recovery", "Recovering metadata") {

  val documentCount = 50
  val location = Location.GCP

  val storedUbams = Seq.fill(documentCount) {
    val flowcellBarcode = s"flowcell$randomId"
    val lane = Random.nextInt()
    val libraryName = s"library$randomId"
    DocumentWgsUbam(
      upsertId = UpsertId.nextId(),
      entityId = Symbol(s"$flowcellBarcode.$lane.$libraryName.${location.entryName}"),
      flowcellBarcode = flowcellBarcode,
      lane = lane,
      libraryName = libraryName,
      location = location,
      ubamPath = Some(URI.create(s"gs://$randomId/$randomId/$randomId")),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }

  val storedGvcfs = Seq.fill(documentCount) {
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val version = Random.nextInt()
    DocumentGvcf(
      upsertId = UpsertId.nextId(),
      entityId = Symbol(s"${location.entryName}.$project.$sampleAlias.$version"),
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = version,
      gvcfPath = Some(URI.create(s"gs://$randomId/$randomId/$randomId")),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }

  val storedWgsCrams = Seq.fill(documentCount) {
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    val version = Random.nextInt()
    DocumentWgsCram(
      upsertId = UpsertId.nextId(),
      entityId = Symbol(s"${location.entryName}.$project.$sampleAlias.$version"),
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = version,
      cramPath = Some(URI.create(s"gs://$randomId/$randomId/$randomId")),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }

  override val container = new ClioDockerComposeContainer(
    new File(getClass.getResource(DockerIntegrationSpec.composeFilename).toURI),
    DockerIntegrationSpec.elasticsearchServiceName,
    Map(
      clioFullName -> DockerIntegrationSpec.clioServicePort,
      esFullName -> DockerIntegrationSpec.elasticsearchServicePort
    ),
    Map(
      ElasticsearchIndex[DocumentWgsUbam] -> storedUbams,
      ElasticsearchIndex[DocumentGvcf] -> storedGvcfs,
      ElasticsearchIndex[DocumentWgsCram] -> storedWgsCrams
    )
  )

  lazy val recoveryDoneFuture: Future[Done] = clioLogLines
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
      status should be(StatusInfo.Running)
    }
  }

  it should "recover wgs-ubam metadata on startup" in {
    for {
      _ <- recoveryDoneFuture
      ubams <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
        ClioCommand.queryWgsUbamName,
        "--location",
        location.entryName
      )
    } yield {
      ubams should have length documentCount.toLong
      ubams.map(_.ubamPath) should contain theSameElementsAs storedUbams.map(
        _.ubamPath
      )
    }
  }

  it should "recover gvcf metadata on startup" in {
    for {
      _ <- recoveryDoneFuture
      gvcfs <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
        ClioCommand.queryGvcfName,
        "--location",
        location.entryName
      )
    } yield {
      gvcfs should have length documentCount.toLong
      gvcfs.map(_.gvcfPath) should contain theSameElementsAs gvcfs.map(
        _.gvcfPath
      )
    }
  }

  it should "recover wgs-cram metadata on startup" in {
    for {
      _ <- recoveryDoneFuture
      crams <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--location",
        location.entryName
      )
    } yield {
      crams should have length documentCount.toLong
      crams.map(_.cramPath) should contain theSameElementsAs crams.map(
        _.cramPath
      )
    }
  }
}
