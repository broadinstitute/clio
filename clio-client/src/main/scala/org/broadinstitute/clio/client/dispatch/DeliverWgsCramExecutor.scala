package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import better.files.File
import org.broadinstitute.clio.client.commands.DeliverWgsCram
import org.broadinstitute.clio.client.dispatch.MoveExecutor.MoveOp
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.transfer.model.wgscram.WgsCramExtensions

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
class DeliverWgsCramExecutor(deliverCommand: DeliverWgsCram)(
  implicit ec: ExecutionContext
) extends MoveExecutor(deliverCommand) {

  override protected def buildMove(
    metadata: moveCommand.index.MetadataType,
    ioUtil: IoUtil
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[MoveOp]), NotUsed] = {

    val baseStream = super
      .buildMove(metadata, ioUtil)
      .orElse(Source.single(metadata -> immutable.Seq.empty))

    baseStream.flatMapConcat {
      case (movedMetadata, moveOps) =>
        (movedMetadata.cramMd5, movedMetadata.cramPath) match {
          case (Some(cramMd5), Some(cramPath)) => {

            val newMetadata =
              movedMetadata.withWorkspaceName(deliverCommand.workspaceName)

            /*
             * FIXME: If we enable the google-cloud-nio adapter in the client,
             * we can write directly to the source location instead of writing
             * to temp and then copying.
             */
            val md5Tmp = File
              .newTemporaryFile("clio-cram-deliver", WgsCramExtensions.Md5Extension)
              .write(cramMd5.name)
              .deleteOnExit()
              .uri
            val cloudMd5Path =
              URI.create(s"$cramPath${WgsCramExtensions.Md5ExtensionAddition}")

            // gsutil complains when trying to rm a local file.
            val op = MoveOp(md5Tmp, cloudMd5Path, deleteSrc = false)
            Source.single(newMetadata -> (moveOps :+ op))
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
}
