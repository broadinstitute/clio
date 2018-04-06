package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.transfer.model.Metadata
import org.broadinstitute.clio.transfer.model.arrays.{ArraysExtensions, ArraysMetadata}

import scala.concurrent.ExecutionContext

/**
  * Special-purpose CLP for delivering arrays to FireCloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic:
  *
  *   1. Copies the idat files to the target path
  *   2. Records the updated locations of the idat files in the metadata
  */
class DeliverArraysExecutor(deliverCommand: DeliverArrays)(implicit ec: ExecutionContext)
    extends MoveExecutor(deliverCommand) {

  override def customMetadataOperations(
    metadata: ArraysMetadata,
    ioUtil: IoUtil
  ): Source[ArraysMetadata, NotUsed] = {
    (metadata.grnIdatPath, metadata.redIdatPath) match {
      case (Some(grn), Some(red)) => {
        val movedGrnIdat = Metadata.findNewPathForMove(
          grn,
          deliverCommand.destination,
          ArraysExtensions.IdatExtension
        )
        val movedRedIdat = Metadata.findNewPathForMove(
          red,
          deliverCommand.destination,
          ArraysExtensions.IdatExtension
        )
        val idatCopies = Map(grn -> movedGrnIdat, red -> movedRedIdat).filterNot {
          case (old, moved) => moved.equals(old)
        }

        ioUtil.copyCloudObjects(idatCopies).map { _ =>
          metadata
            .withWorkspaceName(deliverCommand.workspaceName)
            .copy(
              grnIdatPath = Some(movedGrnIdat),
              redIdatPath = Some(movedRedIdat)
            )
        }
      }
      case (Some(_), None) =>
        Source.failed(
          new IllegalStateException(
            s"Arrays record with key ${deliverCommand.key} is missing its redIdatPath"
          )
        )
      case (None, Some(_)) =>
        Source.failed(
          new IllegalStateException(
            s"Arrays record with key ${deliverCommand.key} is missing its grnIdatPath"
          )
        )
      case _ =>
        Source.failed(
          new IllegalStateException(
            s"Arrays record with key ${deliverCommand.key} is missing both its redIdatPath and its grnIdatPath"
          )
        )
    }
  }
}
