package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.client.commands.BackCompatibleDeliverCram
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{IoOp, WriteOp}
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
    deliveredMetaData: CramMetadata,
    moveOps: immutable.Seq[IoOp]
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed] = {
    (deliveredMetaData.cramMd5, deliveredMetaData.cramPath) match {
      case (Some(cramMd5), Some(cramPath)) => {

        val cloudMd5Path =
          URI.create(s"$cramPath${CramExtensions.Md5ExtensionAddition}")

        Source.single(
          deliveredMetaData -> (moveOps :+ WriteOp(cramMd5.name, cloudMd5Path))
        )
      }
      case _ =>
        Source.failed(
          new IllegalStateException(
            s"Cram record with key ${deliverCommand.key} is missing either its cram path or cram md5"
          )
        )
    }
  }
}
