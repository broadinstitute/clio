package org.broadinstitute.clio.client.dispatch

import java.net.URI

import better.files.File
import org.broadinstitute.clio.client.commands.{DeliverWgsCram, MoveWgsCram}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{WgsCramMetadata, WgsCramExtensions}
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
class DeliverWgsCramExecutor(deliverCommand: DeliverWgsCram) extends Executor[UpsertId] {

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[UpsertId] = {

    val moveCommand = MoveWgsCram(
      deliverCommand.key,
      deliverCommand.workspacePath,
      deliverCommand.newBasename
    )

    val moveExecutor = new MoveExecutor(moveCommand)

    for {
      _ <- moveExecutor.execute(webClient, ioUtil)
      // Metadata must exist at this point because it's required by the move executor.
      Some(cramData) <- webClient.getMetadataForKey(WgsCramIndex)(deliverCommand.key)
      _ <- writeCramMd5(cramData, ioUtil)
      updated = cramData.copy(workspaceName = Some(deliverCommand.workspaceName))
      upsertId <- webClient.upsert(WgsCramIndex)(deliverCommand.key, updated)
    } yield {
      upsertId
    }
  }

  private def writeCramMd5(metadata: WgsCramMetadata, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[Unit] = Future {
    (metadata.cramMd5, metadata.cramPath) match {
      case (Some(cramMd5), Some(cramPath)) => {
        /*
         * FIXME: If we enable the google-cloud-nio adapter in the client,
         * we can write directly to the source location instead of writing
         * to temp and then copying.
         */
        File.usingTemporaryFile("clio-cram-deliver", WgsCramExtensions.Md5Extension) {
          md5Tmp =>
            val cloudMd5Path =
              s"$cramPath${WgsCramExtensions.Md5ExtensionAddition}"

            md5Tmp.write(cramMd5.name)
            val copyRc = ioUtil.copyGoogleObject(
              URI.create(md5Tmp.toString),
              URI.create(cloudMd5Path)
            )
            if (copyRc != 0) {
              throw new RuntimeException(
                s"Failed to copy local cram md5 file to path $cloudMd5Path"
              )
            }
        }
      }
      case _ => {
        throw new IllegalStateException(
          s"Cram record with key ${deliverCommand.key} is missing either its cram path or cram md5"
        )
      }
    }
  }
}
