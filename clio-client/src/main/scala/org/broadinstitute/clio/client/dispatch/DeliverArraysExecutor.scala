package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{CopyOp, IoOp, MoveOp}
import org.broadinstitute.clio.client.util.IoUtil
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

  override protected[dispatch] def buildMove(
    metadata: moveCommand.index.MetadataType,
    ioUtil: IoUtil
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed] = {

    val baseStream = buildDeliveryMove(metadata, ioUtil)
      .orElse(Source.single(metadata -> immutable.Seq.empty))

    baseStream.flatMapConcat {
      case (movedMetadata, moveOps) =>
        buildDelivery(
          movedMetadata.withWorkspaceName(deliverCommand.workspaceName),
          moveOps
        )
    }
  }

  private[dispatch] def buildDeliveryMove(
    metadata: moveCommand.index.MetadataType,
    ioUtil: IoUtil
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed] = {

    val pathsForDelivery = Set("vcfPath", "vcfIndexPath", "gtcPath")
    val preMovePaths = Metadata.extractPaths(metadata) filterKeys pathsForDelivery

    if (preMovePaths.isEmpty) {
      Source.failed(
        new IllegalStateException(
          s"Nothing to move; no files registered to the $name for $prettyKey"
        )
      )
    } else {
      val movedMetadata = metadata.moveInto(destination, moveCommand.newBasename)
      val postMovePaths = Metadata.extractPaths(movedMetadata)

      val opsToPerform = preMovePaths.flatMap {
        case (fieldName, path) => {
          /*
           * Assumptions:
           *   1. If the field exists pre-move, it will still exist post-move
           *   2. If the field is a URI pre-move, it will still be a URI post-move
           */
          Some(MoveOp(path, postMovePaths(fieldName)))
            .filterNot(op => op.src.equals(op.dest))
        }
      }

      Source(opsToPerform)
        .fold(Seq.empty[URI]) {
          case (acc, op) =>
            if (ioUtil.isGoogleObject(op.src)) {
              acc
            } else {
              acc :+ op.src
            }
        }
        .flatMapConcat { nonCloudPaths =>
          if (nonCloudPaths.isEmpty) {
            if (opsToPerform.isEmpty) {
              logger.info(
                s"Nothing to move; all files already exist at $destination and no other metadata changes need applied."
              )
              Source.empty
            } else {
              Source.single(movedMetadata -> opsToPerform.to[immutable.Seq])
            }
          } else {
            Source.failed(
              new IllegalStateException(
                s"""Inconsistent state detected, non-cloud paths registered to the $name for $prettyKey:
                   |${nonCloudPaths.mkString(",")}""".stripMargin
              )
            )
          }
        }
    }
  }
}

object DeliverArraysExecutor {
  val IdatsDir = "idats/"
}
