package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.stream.scaladsl.{Sink, Source}
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.UndeliverCram
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.CramIndex
import org.broadinstitute.clio.transfer.model.cram.{CramKey, CramMetadata}
import org.broadinstitute.clio.util.model.{DataType, Location}
import org.scalamock.scalatest.AsyncMockFactory

class UndeliverExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "DeliverExecutor"

  private val theKey = CramKey(
    location = Location.GCP,
    project = "project",
    sampleAlias = "sample",
    version = 1,
    dataType = DataType.WGS
  )

  private val prodStoragePath = URI.create("gs://prod-storage-path/")
  private val noteForUndeliver: String = "undelivering"

  private val command =
    UndeliverCram(theKey, prodStoragePath, force = false, None, noteForUndeliver)

  type Aux = ClioWebClient.UpsertAux[CramKey, CramMetadata]

  private def testUndeliver(
    metadata: CramMetadata = CramMetadata(
      workspaceName = Option("Workspace"),
      billingProject = Option("BillingProject")
    ),
    force: Boolean = false
  ) = {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(metadata))

    (ioUtil.isGoogleDirectory _).expects(prodStoragePath).returning(true)

    val executor = new UndeliverExecutor(command.copy(force = force))
    executor.checkPreconditions(ioUtil, webClient).runWith(Sink.head).map { m =>
      m should be(metadata)
    }
  }

  it should "not allow a undelivery if a record has no workspace name" in {
    recoverToExceptionIf[Exception] {
      testUndeliver(CramMetadata(workspaceName = Option("")))
    }.map {
      _.getMessage should include("because it is currently not in a workspace.")
    }
  }

  it should "using --force, allow a undelivery if a record workspace name is already empty" in {
    testUndeliver(
      CramMetadata(workspaceName = Option("")),
      force = true
    )
  }

  it should "allow undelivery if a record has been delivered to a workspace" in {
    testUndeliver()
  }
}
