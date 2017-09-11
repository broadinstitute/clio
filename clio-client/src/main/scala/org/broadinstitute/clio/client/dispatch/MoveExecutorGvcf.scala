package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.MoveGvcf
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
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
      _ <- verifyCloudPaths logErrorMsg
        "Clio client can only handle cloud operations right now."
      gvcfPath <- queryForGvcfPath(webClient) logErrorMsg
        "Could not query the Gvcf. No files have been moved."
      _ <- copyGoogleObject(gvcfPath, moveGvcfCommand.metadata.gvcfPath, ioUtil) logErrorMsg
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
      logger.info(s"Successfully moved '${gvcfPath
        .map(name => name)}' to '${moveGvcfCommand.metadata.gvcfPath.map(name => name)}'")
      upsertGvcf
    }
  }

  private def queryForGvcfPath(
    webClient: ClioWebClient
  )(implicit bearerToken: OAuth2BearerToken): Future[Option[String]] = {
    implicit val ec: ExecutionContext = webClient.executionContext

    def ensureOnlyOne(
      gvcfs: Seq[TransferGvcfV1QueryOutput]
    ): TransferGvcfV1QueryOutput = {
      gvcfs.size match {
        case 1 =>
          gvcfs.headOption.getOrElse(
            throw new Exception(
              s"No Gvcfs were found for Key($prettyKey). You can add this Gvcf using the 'add-gvcf' command in the Clio client."
            )
          )
        case s =>
          throw new Exception(
            s"$s Gvcfs were returned for Key($prettyKey), expected 1. You can see what was returned by running the 'query-gvcf' command in the Clio client."
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
        )
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
      .map(_.gvcfPath)
  }

  private def copyGoogleObject(source: Option[String],
                               destination: Option[String],
                               ioUtil: IoUtil): Future[Unit] = {
    ioUtil.copyGoogleObject(source.get, destination.get) match {
      case 0 => Future.successful(())
      case _ =>
        Future.failed(
          new Exception(s"Copy files in the cloud failed from '${source
            .getOrElse("")}' to '${destination.getOrElse("")}'")
        )
    }
  }

  private def deleteGoogleObject(path: Option[String],
                                 ioUtil: IoUtil): Future[Unit] = {
    ioUtil.deleteGoogleObject(path.get) match {
      case 0 => Future.successful(())
      case _ =>
        Future.failed(
          new Exception(
            s"Deleting file in the cloud failed for path '${path.getOrElse("")}'"
          )
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
        transferGvcfV1Metadata = moveGvcfCommand.metadata
      )
      .map(webClient.ensureOkResponse)
  }

  private def prettyKey: String = {
    s"Project: ${moveGvcfCommand.transferGvcfV1Key.project}, " +
      s"SampleAlias: ${moveGvcfCommand.transferGvcfV1Key.sampleAlias}, " +
      s"Version: ${moveGvcfCommand.transferGvcfV1Key.version}, Location: ${moveGvcfCommand.transferGvcfV1Key.location}"
  }

  private def verifyCloudPaths: Future[Unit] = {
    val errorOption = for {
      locationError <- moveGvcfCommand.transferGvcfV1Key.location match {
        case Location.GCP => Some("")
        case _ =>
          Some("Only GCP gvcf are supported at this time.")
      }
      pathError <- moveGvcfCommand.metadata.gvcfPath.map {
        case loc if loc.startsWith("gs://") => ""
        case _ =>
          s"The destination of the gvcf must be a cloud path. ${moveGvcfCommand.metadata.gvcfPath
            .map(name => name)} is not a cloud path."
      }
    } yield {
      if (locationError.isEmpty && pathError.isEmpty) {
        Future.successful(())
      } else {
        Future.failed(new Exception(String.join(" ", locationError, pathError)))
      }
    }
    errorOption.getOrElse(
      Future
        .failed(new Exception("Either location or gvcfPath were not supplied"))
    )
  }
}
