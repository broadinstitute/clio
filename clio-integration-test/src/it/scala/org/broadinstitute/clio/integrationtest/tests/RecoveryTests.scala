package org.broadinstitute.clio.integrationtest.tests

import java.io.File
import java.net.URI

import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.{
  ClioDockerComposeContainer,
  DockerIntegrationSpec
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1QueryOutput
import org.broadinstitute.clio.transfer.model.wgscram.TransferWgsCramV1QueryOutput
import org.broadinstitute.clio.transfer.model.ubam.TransferUbamV1QueryOutput
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}

import scala.util.Random

/** Tests of Clio's recovery mechanisms. */
trait RecoveryTests {
  self: DockerIntegrationSpec =>

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
      ElasticsearchIndex.WgsUbam -> storedUbams,
      ElasticsearchIndex.Gvcf -> storedGvcfs,
      ElasticsearchIndex.WgsCram -> storedWgsCrams
    )
  )

  it should "recover wgs-ubam metadata on startup" in {
    for {
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
