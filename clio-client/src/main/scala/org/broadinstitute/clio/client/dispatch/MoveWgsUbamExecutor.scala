package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.MoveWgsUbam
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken

import scala.concurrent.{ExecutionContext, Future}
class MoveWgsUbamExecutor(moveWgsUbamCommand: MoveWgsUbam) extends Executor {

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    for {
      _ <- verifyCloudPaths logErrorMsg
        "Clio client can only handle cloud operations right now."
      wgsUbamPath <- queryForWgsUbamPath(webClient) logErrorMsg
        "Could not query the WgsUbam. No files have been moved."
      _ <- copyGoogleObject(
        wgsUbamPath,
        moveWgsUbamCommand.metadata.ubamPath,
        ioUtil
      ) logErrorMsg
        "An error occurred while copying the files in the cloud. No files have been moved."
      upsertUbam <- upsertUpdatedWgsUbam(webClient) logErrorMsg
        s"""An error occurred while upserting the WgsUbam.
           |The ubam exists in both at both the old and the new locations.
           |At this time, Clio only knows about the bam at the old location.
           |Try removing the ubam at the new location and re-running this command.
           |If this cannot be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}.
        """.stripMargin
      _ <- deleteGoogleObject(wgsUbamPath, ioUtil) logErrorMsg
        s"""The old bam was not able to be deleted. Clio has been updated to point to the new bam.
           | Please delete the old bam. If this cannot be done, contact Green Team at ${ClioClientConfig.greenTeamEmail}.
           | """.stripMargin
    } yield {
      logger.info(
        s"Successfully moved '${wgsUbamPath.get}' to '${moveWgsUbamCommand.metadata.ubamPath.get}'"
      )
      upsertUbam
    }
  }

  private def queryForWgsUbamPath(
    webClient: ClioWebClient
  )(implicit bearerToken: OAuth2BearerToken): Future[Option[String]] = {
    implicit val ec: ExecutionContext = webClient.executionContext

    def ensureOnlyOne(
      wgsUbams: Seq[TransferWgsUbamV1QueryOutput]
    ): TransferWgsUbamV1QueryOutput = {
      wgsUbams.size match {
        case 1 =>
          wgsUbams.head
        case 0 =>
          throw new Exception(
            s"No WgsUbams were found for Key($prettyKey). You can add this WgsUbam using the 'add-wgs-ubam' command in the Clio client."
          )
        case s =>
          throw new Exception(
            s"$s WgsUbams were returned for Key($prettyKey), expected 1. You can see what was returned by running the 'query-wgs-ubam' command in the Clio client."
          )

      }
    }

    webClient
      .queryWgsUbam(
        TransferWgsUbamV1QueryInput(
          flowcellBarcode =
            Some(moveWgsUbamCommand.transferWgsUbamV1Key.flowcellBarcode),
          lane = Some(moveWgsUbamCommand.transferWgsUbamV1Key.lane),
          libraryName =
            Some(moveWgsUbamCommand.transferWgsUbamV1Key.libraryName),
          location = Option(Location.GCP),
          documentStatus = Option(DocumentStatus.Normal)
        )
      )
      .recoverWith {
        case ex: Exception =>
          Future.failed(
            new Exception("There was an error contacting the Clio server", ex)
          )
      }
      .map(webClient.ensureOkResponse)
      .flatMap(webClient.unmarshal[Seq[TransferWgsUbamV1QueryOutput]])
      .map(ensureOnlyOne)
      .map(_.ubamPath)
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

  private def upsertUpdatedWgsUbam(
    webClient: ClioWebClient
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    implicit val executionContext: ExecutionContext = webClient.executionContext
    webClient
      .addWgsUbam(
        input = moveWgsUbamCommand.transferWgsUbamV1Key,
        transferWgsUbamV1Metadata = moveWgsUbamCommand.metadata
      )
      .map(webClient.ensureOkResponse)
  }

  private def prettyKey: String = {
    s"FlowcellBarcode: ${moveWgsUbamCommand.transferWgsUbamV1Key.flowcellBarcode}, " +
      s"LibraryName: ${moveWgsUbamCommand.transferWgsUbamV1Key.libraryName}, " +
      s"Lane: ${moveWgsUbamCommand.transferWgsUbamV1Key.lane}, Location: ${moveWgsUbamCommand.transferWgsUbamV1Key.location}"
  }

  private def verifyCloudPaths: Future[Unit] = {
    val errorOption = for {
      locationError <- moveWgsUbamCommand.transferWgsUbamV1Key.location match {
        case Location.GCP => Some("")
        case _ =>
          Some("Only GCP unmapped bams are supported at this time.")
      }
      pathError <- moveWgsUbamCommand.metadata.ubamPath.map {
        case loc if loc.startsWith("gs://") => ""
        case _ =>
          s"The destination of the ubam must be a cloud path. ${moveWgsUbamCommand.metadata.ubamPath.get} is not a cloud path."
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
        .failed(new Exception("Either location or ubamPath were not supplied"))
    )
  }
}
