package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{CopyOp, IoOp, MoveOp}
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
    existingMetadata: ArraysMetadata
  ): Source[(ArraysMetadata, immutable.Seq[IoOp]), NotUsed] = {
    (
      existingMetadata.vcfPath,
      existingMetadata.vcfIndexPath,
      existingMetadata.gtcPath,
      existingMetadata.grnIdatPath,
      existingMetadata.redIdatPath
    ) match {
      case (Some(vcf), Some(index), Some(gtc), Some(grn), Some(red)) => {
        val vcfMove = Metadata.buildFilePath(
          vcf,
          deliverCommand.destination,
          ArraysExtensions.VcfGzExtension,
          moveCommand.newBasename
        )
        val vcfIndexMove = Metadata.buildFilePath(
          index,
          deliverCommand.destination,
          ArraysExtensions.VcfGzTbiExtension,
          moveCommand.newBasename
        )
        val gtcMove = Metadata.buildFilePath(
          gtc,
          deliverCommand.destination,
          ArraysExtensions.GtcExtension,
          moveCommand.newBasename
        )
        val moves = immutable
          .Seq(
            MoveOp(vcf, vcfMove),
            MoveOp(index, vcfIndexMove),
            MoveOp(gtc, gtcMove)
          )

        val idatDestination =
          deliverCommand.destination.resolve(DeliverArraysExecutor.IdatsDir)

        val grnCopy = Metadata.buildFilePath(
          grn,
          idatDestination,
          ArraysExtensions.GrnIdatExtension
        )

        val redCopy = Metadata.buildFilePath(
          red,
          idatDestination,
          ArraysExtensions.RedIdatExtension
        )

        val idatCopies = immutable
          .Seq(
            CopyOp(grn, grnCopy),
            CopyOp(red, redCopy)
          )

        val newMetadata = existingMetadata.copy(
          grnIdatPath = Some(grnCopy),
          redIdatPath = Some(redCopy),
          vcfPath = Some(vcfMove),
          vcfIndexPath = Some(vcfIndexMove),
          gtcPath = Some(gtcMove)
        )

        Source.single(newMetadata -> (moves ++ idatCopies))
      }
      case _ =>
        Source.failed(
          new IllegalStateException(
            s"Arrays record with key ${deliverCommand.key} is missing idats, vcf, index or gtc paths"
          )
        )
    }
  }
}

object DeliverArraysExecutor {
  val IdatsDir = "idats/"
}
