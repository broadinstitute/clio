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

/** Tests for recovering documents on startup. Can only run reproducibly in Docker. */
class RecoveryIntegrationSpec
    extends DockerIntegrationSpec("Clio in recovery", "Recovering metadata") {

  val documentCount = 500
  val location = Location.GCP

  def randomUri(i: Int): URI = URI.create(s"gs://$i/$randomId")

  val initUbams: Seq[DocumentWgsUbam] = Seq.tabulate(documentCount) { i =>
    val flowcellBarcode = s"flowcell$randomId"
    val libraryName = s"library$randomId"
    DocumentWgsUbam(
      upsertId = UpsertId.nextId(),
      entityId = Symbol(s"$flowcellBarcode.$i.$libraryName.${location.entryName}"),
      flowcellBarcode = flowcellBarcode,
      lane = i,
      libraryName = libraryName,
      location = location,
      ubamPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }

  val updatedUbams: Seq[DocumentWgsUbam] =
    initUbams.map(
      u => u.copy(upsertId = UpsertId.nextId(), ubamPath = Some(randomUri(u.lane)))
    )

  val initGvcfs: Seq[DocumentGvcf] = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    DocumentGvcf(
      upsertId = UpsertId.nextId(),
      entityId = Symbol(s"${location.entryName}.$project.$sampleAlias.$i"),
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i,
      gvcfPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }

  val updatedGvcfs: Seq[DocumentGvcf] =
    initGvcfs.map(
      g => g.copy(upsertId = UpsertId.nextId(), gvcfPath = Some(randomUri(g.version)))
    )

  val initCrams: Seq[DocumentWgsCram] = Seq.tabulate(documentCount) { i =>
    val project = s"project$randomId"
    val sampleAlias = s"sample$randomId"
    DocumentWgsCram(
      upsertId = UpsertId.nextId(),
      entityId = Symbol(s"${location.entryName}.$project.$sampleAlias.$i"),
      location = location,
      project = project,
      sampleAlias = sampleAlias,
      version = i,
      cramPath = Some(randomUri(i)),
      documentStatus = Some(DocumentStatus.Normal)
    )
  }

  val updatedCrams: Seq[DocumentWgsCram] =
    initCrams.map(
      c => c.copy(upsertId = UpsertId.nextId(), cramPath = Some(randomUri(c.version)))
    )

  override val container = new ClioDockerComposeContainer(
    new File(getClass.getResource(DockerIntegrationSpec.composeFilename).toURI),
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
      status.clio should be(ClioStatus.Started)
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
      val ubamUris = ubams.flatMap(_.ubamPath).sorted
      ubamUris should contain theSameElementsInOrderAs updatedUbams
        .flatMap(_.ubamPath)
        .sorted
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
      val gvcfUris = gvcfs.flatMap(_.gvcfPath).sorted
      gvcfUris should contain theSameElementsInOrderAs updatedGvcfs
        .flatMap(_.gvcfPath)
        .sorted
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
      val cramUris = crams.flatMap(_.cramPath).sorted
      cramUris should contain theSameElementsInOrderAs updatedCrams
        .flatMap(_.cramPath)
        .sorted
    }
  }
}
