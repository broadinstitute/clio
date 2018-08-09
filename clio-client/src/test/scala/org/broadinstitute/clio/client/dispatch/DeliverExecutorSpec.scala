package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.stream.scaladsl.{Sink, Source}
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeliverCram
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.CramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{CramKey, CramMetadata}
import org.broadinstitute.clio.util.model.{DataType, Location}
import org.scalamock.scalatest.AsyncMockFactory

class DeliverExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "DeliverExecutor"

  private val theKey = CramKey(
    location = Location.GCP,
    project = "project",
    sampleAlias = "sample",
    version = 1,
    dataType = DataType.WGS
  )

  private val workspaceName = "workspace"
  private val workspacePath = URI.create("gs://workspacePath/")

  private val command = DeliverCram(theKey, workspaceName, workspacePath)

  type Aux = ClioWebClient.UpsertAux[CramKey, CramMetadata]

  private def testDeliver(
    metadata: CramMetadata = CramMetadata(),
    force: Boolean = false
  ) = {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(metadata))

    (ioUtil.isGoogleDirectory _).expects(workspacePath).returning(true)

    val executor = new DeliverExecutor(command.copy(force = force))
    executor.checkPreconditions(ioUtil, webClient).runWith(Sink.head).map { m =>
      m should be(metadata)
    }
  }

  it should "not allow a delivery if a record has already been delivered" in {
    recoverToExceptionIf[Exception] {
      testDeliver(CramMetadata(workspaceName = Option("existing workspace")))
    }.map {
      _.getMessage should include("has already been delivered")
    }
  }

  it should "using --force, allow a delivery if a record has already been delivered" in {
    testDeliver(
      CramMetadata(workspaceName = Option("existing workspace")),
      force = true
    )
  }

  it should "allow delivery if a record has been delivered if the workspace name is the same" in {
    testDeliver()
  }
}
