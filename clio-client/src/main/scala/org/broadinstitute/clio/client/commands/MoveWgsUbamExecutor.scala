package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.scalalogging.{LazyLogging, Logger}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{TransferWgsUbamV1Key, TransferWgsUbamV1Metadata, TransferWgsUbamV1QueryInput, TransferWgsUbamV1QueryOutput}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.{ExecutionContext, Future}
import org.broadinstitute.clio.client.util.FutureWithErrorMessage
import org.broadinstitute.clio.util.json.ModelAutoDerivation


class MoveWgsUbamExecutor(moveWgsUbamCommand: MoveWgsUbam)
    extends Executor
    with LazyLogging
    with FailFastCirceSupport
    with ModelAutoDerivation
    with FutureWithErrorMessage {

  implicit val implicitLogger: Logger = logger

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    for {
      _ <- verifyPath(webClient) withErrorMsg
        "Clio client can only handle cloud operations right now."
      wgsUbamPath <- queryForWgsUbamPath(webClient) withErrorMsg
        "Could not query the WgsUbam. No files have been moved."
      _ <- copyGoogleObject(wgsUbamPath, config.ubamPath, ioUtil) withErrorMsg
        "An error occurred while copying the files in the cloud. No files have been moved."
      upsertUbam <- upsertUpdatedWgsUbam(webClient, config) withErrorMsg
        s"""An error occurred while upserting the WgsUbam.
           |The ubam exists in both at both the old and the new locations.
           |At this time, Clio only knows about the bam at the old location.
           |Try removing the ubam at the new location and re-running this command.
           |If this cannot be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}.
        """.stripMargin
      _ <- deleteGoogleObject(wgsUbamPath, ioUtil) withErrorMsg
        s"""The old bam was not able to be deleted. Clio has been updated to point to the new bam.
           | Please delete the old bam. If this cannot be done, contact Green Team at ${ClioClientConfig.greenTeamEmail}.
           | """.stripMargin
    } yield {
      logger.info(
        s"Successfully moved '${wgsUbamPath.get}' to '${config.ubamPath.get}'"
      )
      upsertUbam
    }
  }

  private def verifyPath(webClient: ClioWebClient): Future[Unit] = {
    implicit val executionContext: ExecutionContext = webClient.executionContext
    Future {
      config.location.foreach {
        case "GCP" => ()
        case _ =>
          throw new Exception(
            "Only GCP unmapped bams are supported at this time."
          )
      }
      config.ubamPath.foreach {
        case loc if loc.startsWith("gs://") => ()
        case _ =>
          throw new Exception(
            s"The destination of the ubam must be a cloud path. ${config.ubamPath.get} is not a cloud path."
          )
      }
    }
  }

  private def queryForWgsUbamPath(webClient: ClioWebClient): Future[Option[String]] = {
    implicit val ec: ExecutionContext = webClient.executionContext

    def ensureOnlyOne(
      wgsUbams: Seq[TransferWgsUbamV1QueryOutput]
    ): TransferWgsUbamV1QueryOutput = {
      wgsUbams.size match {
        case 1 =>
          wgsUbams.head
        case 0 =>
          throw new Exception(
            s"No WgsUbams were found for Key(${prettyKey(config)}). You can add this WgsUbam using the AddWgsUbam command in the Clio client."
          )
        case s =>
          throw new Exception(
            s"$s WgsUbams were returned for Key(${prettyKey(config)}), expected 1. You can see what was returned by running the QueryWgsUbam command in the Clio client."
          )

      }
    }

    webClient
      .queryWgsUbam(
        config.bearerToken.getOrElse(""),
        TransferWgsUbamV1QueryInput(
          flowcellBarcode = config.flowcell,
          lane = config.lane,
          libraryName = config.libraryName,
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
      .map(ensureOkResponse)
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

  private def upsertUpdatedWgsUbam(webClient: ClioWebClient): Future[HttpResponse] = {
    implicit val executionContext: ExecutionContext = webClient.executionContext
    webClient
      .addWgsUbam(
        bearerToken = config.bearerToken.getOrElse(""),
        input = TransferWgsUbamV1Key(
          flowcellBarcode = config.flowcell.get,
          lane = config.lane.get,
          libraryName = config.libraryName.get,
          location = Location.GCP
        ),
        transferWgsUbamV1Metadata =
          TransferWgsUbamV1Metadata(ubamPath = config.ubamPath)
      )
      .map(ensureOkResponse)
  }

  private def prettyKey: String = {
    s"FlowcellBarcode: ${.flowcell.getOrElse("")}, LibraryName: ${config.libraryName
      .getOrElse("")}, " +
      s"Lane: ${config.lane.getOrElse("")}, Location: ${config.location.getOrElse("")}"
  }

  def ensureOkResponse(httpResponse: HttpResponse): HttpResponse = {
    if (httpResponse.status.isSuccess()) {
      httpResponse
    } else {
      throw new Exception(
        s"Got an error from the Clio server. Status code: ${httpResponse.status}"
      )
    }
  }
}
