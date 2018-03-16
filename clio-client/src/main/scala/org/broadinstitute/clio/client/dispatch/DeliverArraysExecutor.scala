package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.transfer.model.Metadata
import org.broadinstitute.clio.transfer.model.arrays.{ArraysExtensions, ArraysMetadata}

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
    extends MoveExecutor(deliverCommand) {

  override def customMetadataOperations(
    metadata: ArraysMetadata,
    ioUtil: IoUtil
  )(
    implicit ec: ExecutionContext
  ): Future[ArraysMetadata] = Future {
    (metadata.grnIdat, metadata.redIdat) match {
      case (Some(grn), Some(red)) => {
        ioUtil.copyGoogleObject(grn, deliverCommand.destination)
        ioUtil.copyGoogleObject(red, deliverCommand.destination)

        val movedGrnIdat = Metadata.findNewPathForMove(
          metadata.grnIdat.get,
          deliverCommand.destination,
          ArraysExtensions.IdatExtension
        )
        val movedRedIdat = Metadata.findNewPathForMove(
          metadata.redIdat.get,
          deliverCommand.destination,
          ArraysExtensions.IdatExtension
        )

        metadata
          .withWorkspaceName(deliverCommand.workspaceName)
          .copy(
            grnIdat = Some(movedGrnIdat),
            redIdat = Some(movedRedIdat)
          )
      }
      case (Some(_), None) =>
        throw new IllegalStateException(
          s"Arrays record with key ${deliverCommand.key} is missing its redIdatPath"
        )
      case (None, Some(_)) =>
        throw new IllegalStateException(
          s"Arrays record with key ${deliverCommand.key} is missing its grnIdatPath"
        )
      case _ =>
        throw new IllegalStateException(
          s"Arrays record with key ${deliverCommand.key} is missing both its redIdatPath and its grnIdatPath"
        )
    }
  }
}
