package org.broadinstitute.clio.client.dispatch

import java.net.URI
import java.nio.file.Files

import org.broadinstitute.clio.client.commands.{DeliverWgsCram, MoveWgsCram}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput,
  TransferWgsCramV1QueryOutput
}
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.{ExecutionContext, Future}

/**
  * Special-purpose CLP for delivering crams to Firecloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic:
  *
  *   1. Writes the cram md5 value to file at the target path
  *   2. Records the workspace name in the metadata for the delivered cram
  */
class DeliverWgsCramExecutor(deliverCommand: DeliverWgsCram)
    extends Executor[UpsertId] {

  val moveCommand =
    MoveWgsCram(deliverCommand.key, deliverCommand.workspacePath)
  import moveCommand.index.implicits._

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[UpsertId] = {

    for {
      _ <- ensureMove(webClient, ioUtil)
      _ <- writeCramMd5(webClient, ioUtil)
        .logErrorMsg("Failed to write cram md5 to file")
      upsertId <- recordWorkspaceName(webClient)
        .logErrorMsg("Failed to records workspace name in cram metadata")
    } yield {
      upsertId
    }
  }

  private def ensureMove(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[UpsertId] = {
    val moveExecutor = new MoveExecutor(moveCommand)
    moveExecutor.execute(webClient, ioUtil).map {
      _.getOrElse {
        throw new IllegalStateException(
          s"The ${moveExecutor.name} for ${moveExecutor.prettyKey} has no files to move, and can't be delivered"
        )
      }
    }
  }

  private def writeCramMd5(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[Unit] = {
    val queryInput = TransferWgsCramV1QueryInput(
      location = Some(deliverCommand.key.location),
      project = Some(deliverCommand.key.project),
      sampleAlias = Some(deliverCommand.key.sampleAlias),
      version = Some(deliverCommand.key.version)
    )

    for {
      queryResponse <- webClient.query(WgsCramIndex)(
        queryInput,
        includeDeleted = false
      )
    } yield {
      val outputs = queryResponse
        .as[Seq[TransferWgsCramV1QueryOutput]]
        .fold(throw _, identity)
      // If there was more than one cram in Clio, the original move would have failed.
      val cramData = outputs.head

      (cramData.cramMd5, cramData.cramPath) match {
        case (Some(cramMd5), Some(cramPath)) => {

          val md5Tmp = Files.createTempFile("clio-cram-deliver", "md5")
          val cloudMd5Path = s"$cramPath.md5"

          Files.write(md5Tmp, cramMd5.name.getBytes)
          val copyRc = ioUtil.copyGoogleObject(
            URI.create(md5Tmp.toString),
            URI.create(cloudMd5Path)
          )
          Files.delete(md5Tmp)
          if (copyRc != 0) {
            throw new RuntimeException(
              s"Failed to copy local cram md5 file to path $cloudMd5Path"
            )
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

  private def recordWorkspaceName(
    webClient: ClioWebClient
  ): Future[UpsertId] = {
    val newMetadata = TransferWgsCramV1Metadata(
      workspaceName = Some(deliverCommand.workspaceName)
    )
    webClient.upsert(WgsCramIndex)(deliverCommand.key, newMetadata)
  }
}
