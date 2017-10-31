package org.broadinstitute.clio.client.dispatch

import java.net.URI
import java.nio.file.Files

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpCredentials
import org.broadinstitute.clio.client.commands.{DeliverWgsCram, MoveWgsCram}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput,
  TransferWgsCramV1QueryOutput
}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Special-purpose CLP for delivering crams to Firecloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic.
  */
class DeliverExecutor(deliverCommand: DeliverWgsCram) extends Executor {

  val moveCommand =
    MoveWgsCram(deliverCommand.key, deliverCommand.workspacePath)
  import moveCommand.index.implicits._

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ex: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {
    import webClient.materializer

    val moveExecutor = new MoveExecutor(moveCommand)

    for {
      moveResponse <- moveExecutor.execute(webClient, ioUtil)
      _ = moveResponse.discardEntityBytes()
      _ <- writeCramMd5(webClient, ioUtil)
      upsertResponse <- recordWorkspaceName(webClient)
    } yield {
      upsertResponse
    }
  }

  private def writeCramMd5(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ex: ExecutionContext,
    credentials: HttpCredentials
  ): Future[Unit] = {
    val queryInput = TransferWgsCramV1QueryInput(
      location = Some(deliverCommand.key.location),
      project = Some(deliverCommand.key.project),
      sampleAlias = Some(deliverCommand.key.sampleAlias),
      version = Some(deliverCommand.key.version)
    )

    for {
      queryResponse <- webClient.query(
        WgsCramIndex,
        queryInput,
        includeDeleted = false
      )
      outputs <- webClient.unmarshal[Seq[TransferWgsCramV1QueryOutput]](
        queryResponse
      )
    } yield {
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
  )(implicit credentials: HttpCredentials): Future[HttpResponse] = {
    val newMetadata = TransferWgsCramV1Metadata(
      workspaceName = Some(deliverCommand.workspaceName)
    )
    webClient.upsert(WgsCramIndex, deliverCommand.key, newMetadata)
  }
}
