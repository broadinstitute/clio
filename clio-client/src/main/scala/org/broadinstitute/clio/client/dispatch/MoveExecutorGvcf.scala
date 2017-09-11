package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.{ClioCommand, MoveGvcf}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput,
  TransferGvcfV1QueryOutput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.{ExecutionContext, Future}

class MoveExecutorGvcf(moveGvcfCommand: MoveGvcf) extends Executor {

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    for {
      _ <- Future(verifyCloudPaths(ioUtil)) logErrorMsg
        "Clio client can only handle cloud operations right now."
      gvcfPath <- queryForGvcfPath(webClient) logErrorMsg
        "Could not query the Gvcf. No files have been moved."
      _ <- copyGoogleObject(gvcfPath, moveGvcfCommand.destination, ioUtil) logErrorMsg
        "An error occurred while copying the files in the cloud. No files have been moved."
      upsertGvcf <- upsertUpdatedGvcf(webClient) logErrorMsg
        s"""An error occurred while upserting the Gvcf.
           |The gvcf exists in both at both the old and the new locations.
           |At this time, Clio only knows about the bam at the old location.
           |Try removing the gvcf at the new location and re-running this command.
           |If this cannot be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}.
        """.stripMargin
      _ <- deleteGoogleObject(gvcfPath, ioUtil) logErrorMsg
        s"""The old bam was not able to be deleted. Clio has been updated to point to the new bam.
           | Please delete the old bam. If this cannot be done, contact Green Team at ${ClioClientConfig.greenTeamEmail}.
           | """.stripMargin
    } yield {
      logger.info(
        s"Successfully moved '$gvcfPath' to '${moveGvcfCommand.destination}'"
      )
      upsertGvcf
    }
  }

  private def queryForGvcfPath(
    webClient: ClioWebClient
  )(implicit bearerToken: OAuth2BearerToken): Future[String] = {
    implicit val ec: ExecutionContext = webClient.executionContext

    def ensureOnlyOne(
      gvcfs: Seq[TransferGvcfV1QueryOutput]
    ): TransferGvcfV1QueryOutput = {
      gvcfs.size match {
        case 1 =>
          gvcfs.headOption.getOrElse(
            throw new Exception(
              s"No Gvcfs were found for Key($prettyKey). You can add this Gvcf using the '${ClioCommand.addGvcfName}' command in the Clio client."
            )
          )
        case s =>
          throw new Exception(
            s"$s Gvcfs were returned for Key($prettyKey), expected 1. You can see what was returned by running the '${ClioCommand.queryGvcfName}' command in the Clio client."
          )

      }
    }

    webClient
      .queryGvcf(
        TransferGvcfV1QueryInput(
          documentStatus = Option(DocumentStatus.Normal),
          location = Option(Location.GCP),
          project = Option(moveGvcfCommand.transferGvcfV1Key.project),
          sampleAlias = Option(moveGvcfCommand.transferGvcfV1Key.sampleAlias),
          version = Option(moveGvcfCommand.transferGvcfV1Key.version)
        ),
        includeDeleted = false
      )
      .recoverWith {
        case ex: Exception =>
          Future.failed(
            new Exception("There was an error contacting the Clio server", ex)
          )
      }
      .map(webClient.ensureOkResponse)
      .flatMap(webClient.unmarshal[Seq[TransferGvcfV1QueryOutput]])
      .map(ensureOnlyOne)
      .map {
        _.gvcfPath.getOrElse {
          throw new Exception(
            s"The gvcf for Key($prettyKey) has no registered path, and can't be moved."
          )
        }
      }
  }

  private def copyGoogleObject(source: String,
                               destination: String,
                               ioUtil: IoUtil): Future[Unit] = {
    ioUtil.copyGoogleObject(source, destination) match {
      case 0 => Future.successful(())
      case _ =>
        Future.failed(
          new Exception(
            s"Copy files in the cloud failed from '$source' to '$destination'"
          )
        )
    }
  }

  private def deleteGoogleObject(path: String, ioUtil: IoUtil): Future[Unit] = {
    ioUtil.deleteGoogleObject(path) match {
      case 0 => Future.successful(())
      case _ =>
        Future.failed(
          new Exception(s"Deleting file in the cloud failed for path '$path'")
        )
    }
  }

  private def upsertUpdatedGvcf(
    webClient: ClioWebClient
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    implicit val executionContext: ExecutionContext = webClient.executionContext
    webClient
      .addGvcf(
        input = moveGvcfCommand.transferGvcfV1Key,
        transferGvcfV1Metadata =
          TransferGvcfV1Metadata(gvcfPath = Some(moveGvcfCommand.destination))
      )
      .map(webClient.ensureOkResponse)
  }

  private def prettyKey: String = {
    s"Project: ${moveGvcfCommand.transferGvcfV1Key.project}, " +
      s"SampleAlias: ${moveGvcfCommand.transferGvcfV1Key.sampleAlias}, " +
      s"Version: ${moveGvcfCommand.transferGvcfV1Key.version}, Location: ${moveGvcfCommand.transferGvcfV1Key.location}"
  }

  private def verifyCloudPaths(ioUtil: IoUtil): Unit = {
    if (moveGvcfCommand.transferGvcfV1Key.location != Location.GCP) {
      throw new Exception("Only GCP gvcfs are supported at this time.")
    } else if (!ioUtil.isGoogleObject(moveGvcfCommand.destination)) {
      throw new Exception(
        s"The destination of the gvcf must be a cloud path. ${moveGvcfCommand.destination} is not a cloud path."
      )
    }
  }
}
