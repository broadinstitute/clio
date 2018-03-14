package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.transfer.model.arrays.{ArraysExtensions, TransferArraysV1Metadata}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Special-purpose CLP for delivering arrays to FireCloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic:
  *
  *   1. Writes the arrays md5 value to file at the target path
  *   2. Records the workspace name in the metadata for the delivered arrays
  */
class DeliverArraysExecutor(deliverCommand: DeliverArrays)
    extends DeliverExecutor(deliverCommand) {

  override def customMetadataOperations(metadata: TransferArraysV1Metadata, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[TransferArraysV1Metadata] = {
    metadata.grnIdat match {
      case Some(path) => ioUtil.copyGoogleObject(path, deliverCommand.destination)
      case None => throw new IllegalStateException(
        s"Arrays record with key ${deliverCommand.key} is missing its grnIdatPath"
      )
    }
    metadata.redIdat match {
      case Some(path) => ioUtil.copyGoogleObject(path, deliverCommand.destination)
      case None => throw new IllegalStateException(
        s"Arrays record with key ${deliverCommand.key} is missing its redIdatPath"
      )
    }

    val movedGrnIdat = TransferMetadata.findNewPathForMove(metadata.grnIdat.get, deliverCommand.destination, ArraysExtensions.IdatExtension)
    val movedRedIdat = TransferMetadata.findNewPathForMove(metadata.redIdat.get, deliverCommand.destination, ArraysExtensions.IdatExtension)

    Future{metadata.withMovedIdats(movedGrnIdat, movedRedIdat)}
  }
}
