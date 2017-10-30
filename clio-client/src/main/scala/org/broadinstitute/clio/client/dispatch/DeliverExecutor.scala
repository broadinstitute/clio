package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpCredentials
import org.broadinstitute.clio.client.commands.{DeliverWgsCram, MoveWgsCram}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.TransferWgsCramV1Metadata

import scala.concurrent.{ExecutionContext, Future}

/**
  * Special-purpose CLP for delivering crams to Firecloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic.
  */
class DeliverExecutor(deliverCommand: DeliverWgsCram) extends Executor {

  val moveCommand =
    MoveWgsCram(deliverCommand.key, deliverCommand.workspacePath)
  import moveCommand.index.implicits._

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ex: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {
    import webClient.materializer

    val moveExecutor = new MoveExecutor(moveCommand)

    for {
      moveResponse <- moveExecutor.execute(webClient, ioUtil)
      _ = moveResponse.discardEntityBytes()
      upsertResponse <- recordWorkspaceName(webClient)
    } yield {
      upsertResponse
    }
  }

  private def recordWorkspaceName(
    webClient: ClioWebClient
  )(implicit credentials: HttpCredentials): Future[HttpResponse] = {
    val newMetadata = TransferWgsCramV1Metadata(
      workspaceName = Some(deliverCommand.workspaceName)
    )
    webClient.upsert(WgsCramIndex, deliverCommand.key, newMetadata)
  }
}
