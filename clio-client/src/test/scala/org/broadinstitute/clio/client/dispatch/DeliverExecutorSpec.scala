package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeliverWgsCram
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{WgsCramKey, WgsCramMetadata}
import org.broadinstitute.clio.util.model.Location
import org.scalamock.scalatest.AsyncMockFactory

import scala.collection.immutable

class DeliverExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "DeliverExecutor"

  private val theKey = WgsCramKey(
    location = Location.GCP,
    project = "project",
    sampleAlias = "sample",
    version = 1
  )

  private val workspaceName = "workspace"
  private val workspacePath = URI.create("gs://workspacePath/")

  private val command = DeliverWgsCram(theKey, workspaceName, workspacePath)

  type Aux = ClioWebClient.UpsertAux[WgsCramKey, WgsCramMetadata]

  private def testDeliver(
    metadata: WgsCramMetadata = WgsCramMetadata(),
    force: Boolean = false
  ) = {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: WgsCramKey, _: Boolean))
      .expects(WgsCramIndex, theKey, false)
      .returning(Source.single(metadata))

    (ioUtil.isGoogleDirectory _).expects(workspacePath).returning(true)

    val executor =
      new DeliverExecutor(command.copy(force = force)) {
        override protected def buildDelivery(
          metadata: WgsCramMetadata,
          moveOps: immutable.Seq[MoveExecutor.IoOp]
        ): Source[(WgsCramMetadata, immutable.Seq[MoveExecutor.IoOp]), NotUsed] =
          Source.single(metadata -> moveOps)
      }
    executor.checkPreconditions(ioUtil, webClient).runWith(Sink.head).map { m =>
      m should be(metadata)
    }
  }

  it should "not allow a delivery if a record has already been delivered" in {
    recoverToExceptionIf[Exception] {
      testDeliver(WgsCramMetadata(workspaceName = Option("existing workspace")))
    }.map {
      _.getMessage should include("has already been delivered")
    }
  }

  it should "using --force, allow a delivery if a record has already been delivered" in {
    testDeliver(
      WgsCramMetadata(workspaceName = Option("existing workspace")),
      force = true
    )
  }

  it should "allow delivery if a record has been delivered if the workspace name is the same" in {
    testDeliver()
  }
}
