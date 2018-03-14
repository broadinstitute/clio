package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands.DeliverCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.DeliverableIndex
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.{ExecutionContext, Future}

/**
  * Special-purpose CLP for delivering crams to FireCloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic:
  *
  *   1. Writes the cram md5 value to file at the target path
  *   2. Records the workspace name in the metadata for the delivered cram
  */
abstract class DeliverExecutor[DI <: DeliverableIndex](deliverCommand: DeliverCommand[DI])
    extends MoveExecutor[DI](deliverCommand) {

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[Option[UpsertId]] = {
    for {
      _ <- super.execute(webClient, ioUtil)
      // Metadata must exist at this point because it's required by the move executor.
      Some(data) <- webClient.getMetadataForKey(deliverCommand.index)(deliverCommand.key)
      _ <- writeMd5(data, ioUtil)
      updated = data.withWorkspaceName(deliverCommand.workspaceName)
      upsertId <- webClient.upsert(deliverCommand.index)(deliverCommand.key, updated)
    } yield {
      Some(upsertId)
    }
  }

  protected def writeMd5(metadata: deliverCommand.index.MetadataType, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[Unit]
}
