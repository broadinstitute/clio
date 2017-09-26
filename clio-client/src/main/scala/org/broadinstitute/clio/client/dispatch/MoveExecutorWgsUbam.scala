package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpCredentials
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.{ClioCommand, MoveWgsUbam}
import org.broadinstitute.clio.client.util.{ClassUtil, IoUtil}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.{ExecutionContext, Future}

class MoveExecutorWgsUbam(moveWgsUbamCommand: MoveWgsUbam) extends Executor {

  private val prettyKey =
    ClassUtil.formatFields(moveWgsUbamCommand.transferWgsUbamV1Key)

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {
    for {
      _ <- Future(verifyCloudPaths(ioUtil)) logErrorMsg
        "Clio client can only handle cloud operations right now."
      wgsUbamPath <- queryForWgsUbamPath(webClient) logErrorMsg
        "Could not query the wgs-ubam. No files have been moved."
      _ <- copyGoogleObject(wgsUbamPath, moveWgsUbamCommand.destination, ioUtil) logErrorMsg
        "An error occurred while copying the files in the cloud. No files have been moved."
      upsertUbam <- upsertUpdatedWgsUbam(webClient) logErrorMsg
        s"""An error occurred while upserting the wgs-ubam.
           |The wgs-ubam exists in both at both the old and the new locations.
           |At this time, Clio only knows about the wgs-ubam at the old location.
           |Try removing the wgs-ubam at the new location and re-running this command.
           |If this cannot be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}.
        """.stripMargin
      _ <- deleteGoogleObject(wgsUbamPath, ioUtil) logErrorMsg
        s"""The old wgs-ubam was not able to be deleted. Clio has been updated to point to the new wgs-ubam.
           |Please delete the old wgs-ubam. If this cannot be done, contact Green Team at ${ClioClientConfig.greenTeamEmail}.
         """.stripMargin
    } yield {
      logger.info(
        s"Successfully moved '$wgsUbamPath' to '${moveWgsUbamCommand.destination}'"
      )
      upsertUbam
    }
  }

  private def queryForWgsUbamPath(
    webClient: ClioWebClient
  )(implicit credentials: HttpCredentials): Future[String] = {
    implicit val ec: ExecutionContext = webClient.executionContext

    def ensureOnlyOne(
      wgsUbams: Seq[TransferWgsUbamV1QueryOutput]
    ): TransferWgsUbamV1QueryOutput = {
      wgsUbams.size match {
        case 1 =>
          wgsUbams.head
        case 0 =>
          throw new Exception(
            s"No wgs-ubams were found for $prettyKey. You can add this wgs-ubam using the '${ClioCommand.addWgsUbamName}' command in the Clio client."
          )
        case s =>
          throw new Exception(
            s"$s wgs-ubams were returned for $prettyKey, expected 1. You can see what was returned by running the '${ClioCommand.queryWgsUbamName}' command in the Clio client."
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
          location = Option(Location.GCP)
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
      .flatMap(webClient.unmarshal[Seq[TransferWgsUbamV1QueryOutput]])
      .map(ensureOnlyOne)
      .map {
        _.ubamPath.getOrElse {
          throw new Exception(
            s"The wgs-ubam for $prettyKey has no registered path, and can't be moved."
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

  private def upsertUpdatedWgsUbam(
    webClient: ClioWebClient
  )(implicit credentials: HttpCredentials): Future[HttpResponse] = {
    implicit val executionContext: ExecutionContext = webClient.executionContext
    webClient
      .upsertWgsUbam(
        key = moveWgsUbamCommand.transferWgsUbamV1Key,
        metadata = TransferWgsUbamV1Metadata(
          ubamPath = Some(moveWgsUbamCommand.destination)
        )
      )
      .map(webClient.ensureOkResponse)
  }

  private def verifyCloudPaths(ioUtil: IoUtil): Unit = {
    if (moveWgsUbamCommand.transferWgsUbamV1Key.location != Location.GCP) {
      throw new Exception(
        "Only GCP unmapped wgs-ubams are supported at this time."
      )
    }

    if (!ioUtil.isGoogleObject(moveWgsUbamCommand.destination)) {
      throw new Exception(
        s"The destination of the wgs-ubam must be a cloud path. ${moveWgsUbamCommand.destination} is not a cloud path."
      )
    }
  }
}
