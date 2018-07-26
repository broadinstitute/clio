package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.client.commands.BackCompatibleDeliverCram
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{IoOp, MoveOp, WriteOp}
import org.broadinstitute.clio.transfer.model.Metadata
import org.broadinstitute.clio.transfer.model.wgscram.{CramExtensions, CramMetadata}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/**
  * Special-purpose CLP for delivering crams to FireCloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic:
  *
  *   1. Writes the cram md5 value to file at the target path
  *   2. Records the workspace name in the metadata for the delivered cram
  */
class DeliverCramExecutor(deliverCommand: BackCompatibleDeliverCram)(
  implicit ec: ExecutionContext
) extends DeliverExecutor(deliverCommand) {

  override protected def buildDelivery(
    deliveredMetadata: CramMetadata
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed] = {
    (deliveredMetadata.cramMd5, deliveredMetadata.cramPath, deliveredMetadata.craiPath) match {
      case (Some(md5), Some(cram), Some(crai)) => {
        val cramMove = Metadata.buildFilePath(
          cram,
          deliverCommand.destination,
          CramExtensions.CramExtension,
          moveCommand.newBasename
        )
        val craiMove = Metadata.buildFilePath(
          crai,
          deliverCommand.destination,
          CramExtensions.CraiExtension,
          moveCommand.newBasename
        )
        val cloudMd5Path =
          URI.create(s"$cramMove${CramExtensions.Md5ExtensionAddition}")

        val moves = immutable
          .Seq(
            MoveOp(cram, cramMove),
            MoveOp(crai, craiMove)
          )
          .filterNot { op =>
            op.src.equals(op.dest)
          }

        val newMetadata = deliveredMetadata.copy(
          cramPath = Some(cramMove),
          craiPath = Some(craiMove),
          cramMd5 = Some(md5)
        )

        Source.single(
          newMetadata -> (moves :+ WriteOp(md5.name, cloudMd5Path))
        )

      }
      case _ =>
        Source.failed(
          new IllegalStateException(
            s"Cram record with key ${deliverCommand.key} is missing either its cram path, crai path or cram md5"
          )
        )
    }
  }
}
