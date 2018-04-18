package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeliverWgsCram
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  WgsCramExtensions,
  WgsCramKey,
  WgsCramMetadata
}
import org.broadinstitute.clio.util.model.{Location, UpsertId}
import org.scalamock.scalatest.AsyncMockFactory

import scala.collection.immutable
import scala.concurrent.ExecutionContext

class DeliverExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "DeliverExecutor"

  private val theKey = WgsCramKey(
    location = Location.GCP,
    project = "project",
    sampleAlias = "sample",
    version = 1
  )
  private val cramPath: URI = URI.create("gs://the-cram")
  private val craiPath: URI = URI.create("gs://the-crai")
  private val metadata = WgsCramMetadata(
    cramPath = Some(cramPath),
    craiPath = Some(craiPath),
    cramMd5 = Some(Symbol("md5"))
  )
  private val workspaceName = "workspace"
  private val workspacePath = URI.create("gs://workspacePath/")
  private val id = UpsertId.nextId()
  private val newBasename = "new"

  type Aux = ClioWebClient.UpsertAux[WgsCramKey, WgsCramMetadata]

  private def initMocks(metadata: WgsCramMetadata) = {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: WgsCramKey, _: Boolean))
      .expects(WgsCramIndex, theKey, false)
      .returning(Source.single(metadata))

    (ioUtil.isGoogleDirectory _).expects(workspacePath).returning(true)

    val paths = Seq(cramPath, craiPath).to[immutable.Iterable]
    paths.foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
    }
    (ioUtil
      .deleteCloudObjects(_: immutable.Iterable[URI])(_: ExecutionContext))
      .expects(paths, executionContext)
      .returning(Source.single(()))

    val destinationCramPath =
      URI.create(workspacePath + newBasename + WgsCramExtensions.CramExtension)
    val destinationCraiPath =
      URI.create(workspacePath + newBasename + WgsCramExtensions.CraiExtension)
    val destinationMd5Path =
      URI.create(workspacePath + newBasename + WgsCramExtensions.Md5Extension)

    (ioUtil.copyGoogleObject _).expects(cramPath, destinationCramPath)
    (ioUtil.copyGoogleObject _).expects(craiPath, destinationCraiPath)
    (ioUtil.copyGoogleObject _).expects(*, destinationMd5Path)

    (webClient
      .upsert(_: Aux)(_: WgsCramKey, _: WgsCramMetadata, _: Boolean))
      .expects(
        WgsCramIndex,
        theKey,
        metadata.copy(
          cramPath = Some(destinationCramPath),
          craiPath = Some(destinationCraiPath),
          workspaceName = Some(workspaceName)
        ),
        true
      )
      .returning(Source.single(id.asJson))
    (ioUtil, webClient)
  }

  private def successfulDelivery(
    metadata: WgsCramMetadata = metadata,
    force: Boolean = false
  ) = {
    val (ioUtil: IoUtil, webClient: ClioWebClient) = initMocks(metadata)

    val executor =
      new DeliverWgsCramExecutor(
        DeliverWgsCram(
          theKey,
          workspaceName,
          workspacePath,
          newBasename = Some(newBasename),
          force = force
        )
      )
    executor.execute(webClient, ioUtil).runWith(Sink.head).map { json =>
      json.as[UpsertId] should be(Right(id))
    }
  }

  it should "deliver wgs cram with base name change" in {
    successfulDelivery()
  }

  it should "not allow a delivery if a record has already been delivered" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: WgsCramKey, _: Boolean))
      .expects(WgsCramIndex, theKey, false)
      .returning(
        Source.single(metadata.copy(workspaceName = Option("existing workspace")))
      )

    (ioUtil.isGoogleDirectory _).expects(workspacePath).returning(true)

    val executor =
      new DeliverWgsCramExecutor(
        DeliverWgsCram(
          theKey,
          workspaceName,
          workspacePath,
          newBasename = Some(newBasename)
        )
      )
    recoverToExceptionIf[Exception] {
      executor.execute(webClient, ioUtil).runWith(Sink.head)
    }.map {
      _.getMessage should include("has already been delivered")
    }
  }

  it should "using --force, allow a delivery if a record has already been delivered" in {
    successfulDelivery(
      metadata = metadata.copy(workspaceName = Option("existing workspace")),
      force = true
    )
  }

  it should "allow delivery if a record has been delivered if the workspace name is the same" in {
    successfulDelivery(metadata = metadata.copy(workspaceName = Option(workspaceName)))
  }
}
