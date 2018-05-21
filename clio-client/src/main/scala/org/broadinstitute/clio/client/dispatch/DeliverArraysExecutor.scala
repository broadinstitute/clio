package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{CopyOp, IoOp}
import org.broadinstitute.clio.transfer.model.Metadata
import org.broadinstitute.clio.transfer.model.arrays.{ArraysExtensions, ArraysMetadata}

import scala.collection.immutable
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
    extends DeliverExecutor(deliverCommand) {

  override protected[dispatch] def buildDelivery(
    deliveredMetadata: ArraysMetadata,
    moveOps: immutable.Seq[IoOp]
  ): Source[(ArraysMetadata, immutable.Seq[IoOp]), NotUsed] = {
    (deliveredMetadata.grnIdatPath, deliveredMetadata.redIdatPath) match {
      case (Some(grn), Some(red)) => {
        val idatDestination =
          deliverCommand.destination.resolve(DeliverArraysExecutor.IdatsDir)

        val grnCopy = Metadata.buildFilePath(
          grn,
          idatDestination,
          ArraysExtensions.IdatExtension
        )

        val redCopy = Metadata.buildFilePath(
          red,
          idatDestination,
          ArraysExtensions.IdatExtension
        )

        val idatCopies = immutable
          .Seq(
            CopyOp(grn, grnCopy),
            CopyOp(red, redCopy)
          )
          .filterNot { op =>
            op.src.equals(op.dest)
          }

        val newMetadata = deliveredMetadata.copy(
          grnIdatPath = Some(grnCopy),
          redIdatPath = Some(redCopy)
        )

        Source.single(newMetadata -> (moveOps ++ idatCopies))
      }
      case (Some(_), None) =>
        Source.failed(
          new IllegalStateException(
            s"Arrays record with key ${deliverCommand.key} is missing its red idat path"
          )
        )
      case (None, Some(_)) =>
        Source.failed(
          new IllegalStateException(
            s"Arrays record with key ${deliverCommand.key} is missing its grn idat path"
          )
        )
      case _ =>
        Source.failed(
          new IllegalStateException(
            s"Arrays record with key ${deliverCommand.key} is missing both its idat paths"
          )
        )
    }
  }
}

object DeliverArraysExecutor {
  val IdatsDir = "idats/"
}
