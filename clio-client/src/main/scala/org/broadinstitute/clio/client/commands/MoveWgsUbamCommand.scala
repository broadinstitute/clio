package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.{LazyLogging, Logger}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Json
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.{ExecutionContext, Future}
import org.broadinstitute.clio.client.util.FutureWithErrorMessage.FutureWithErrorMessage
import org.broadinstitute.clio.util.json.ModelAutoDerivation

object MoveWgsUbamCommand
    extends Command
    with LazyLogging
    with FailFastCirceSupport
    with ModelAutoDerivation {

  implicit val implicitLogger: Logger = logger

  override def execute(
    webClient: ClioWebClient,
    config: BaseArgs,
    ioUtil: IoUtil
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    for {
      _ <- verifyPath(webClient, config) withErrorMsg
        "Clio client can only handle cloud operations right now."
      wgsUbamPath <- queryForWgsUbamPath(webClient, config) withErrorMsg
        "Could not query the WgsUbam. No files have been moved."
      _ <- copyGoogleObject(wgsUbamPath, config.ubamPath, ioUtil) withErrorMsg
        "An error occurred while copying the files in google cloud. No files have been moved."
      upsertUbam <- upsertUpdatedWgsUbam(webClient, config) withErrorMsg
        """An error occurred while upserting the WgsUbam. The state of clio is now inconsistent.
          |The ubam exists in both at both the old and the new locations.
          |At this time, Clio only knows about the bam at the old location.
          |Then, try upserting (not moving) the unmapped bam with the correct path then deleting the old bam.
          |If the state of Clio cannot be fixed, please contact the Green Team at greenteam@broadinstitute.org.
        """.stripMargin
      _ <- deleteGoogleObject(wgsUbamPath, ioUtil) withErrorMsg
        """The old bam was not able to be deleted. Clio has been updated to point to the new bam.
          | Please delete the old bam. If this cannot be done, contact Green Team at greenteam@broadinstitute.org.
          | """.stripMargin
    } yield {
      logger.info(
        s"Successfully moved '${wgsUbamPath.get}' to '${config.ubamPath.get}'"
      )
      upsertUbam
    }
  }

  private def verifyPath(webClient: ClioWebClient,
                         config: BaseArgs): Future[Unit] = {
    implicit val executionContext: ExecutionContext = webClient.executionContext
    Future(config.location.foreach {
      case "GCP" => ()
      case _ =>
        logger.error("Only GCP unmapped bams are supported at this time.")
        throw new Exception()
    }).flatMap { _ =>
      Future(config.ubamPath.foreach {
        case loc if loc.startsWith("gs://") => ()
        case _ =>
          logger.error(
            s"The destination of the ubam must be a cloud path. ${config.ubamPath.get} is not a cloud path."
          )
          throw new Exception()
      })
    }
  }

  private def queryForWgsUbamPath(webClient: ClioWebClient,
                                  config: BaseArgs): Future[Option[String]] = {
    implicit val ec: ExecutionContext = webClient.executionContext

    def ensureOnlyOneJson(wgsUbams: Json): Json = {
      wgsUbams.asArray.getOrElse(throw new Exception()).size match {
        case 1 =>
          wgsUbams.asArray.get.head
        case s if s > 1 =>
          logger.error(
            s"$s WgsUbams were returned for Key(${prettyKey(config)}), expected 1."
          )
          throw new Exception()
        case s if s == 0 =>
          logger.error(s"No WgsUbams were found for Key(${prettyKey(config)})")
          throw new Exception()
      }
    }

    // Using JSON instead of TransferWgsUbamV1QueryOutput here on purpose.
    // Currently, the clio server spits json out in snake_case.
    // Clio client expects it json keys to be in camelCase.
    // This should be changed to use the transfer model once the issue is fixed
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
          logger.error("There was an error contacting the Clio server")
          Future.failed(new Exception(ex))
      }
      .map(ensureOkResponse)
      .flatMap(webClient.unmarshal[Json])
      .map(ensureOnlyOneJson)
      .map(_.asObject.get("ubam_path").get.asString)
  }

  private def copyGoogleObject(source: Option[String],
                               destination: Option[String],
                               ioUtil: IoUtil): Future[Unit] = {
    ioUtil.copyGoogleObject(source.get, destination.get) match {
      case 0 => Future.successful(())
      case _ =>
        logger.error(s"Copy files in google cloud failed from '${source
          .getOrElse("")}' to '${destination.getOrElse("")}'")
        Future.failed(new Exception())
    }
  }

  private def deleteGoogleObject(path: Option[String],
                                 ioUtil: IoUtil): Future[Unit] = {
    ioUtil.deleteGoogleObject(path.get) match {
      case 0 => Future.successful(())
      case _ =>
        logger.error(
          s"Deleting file in google cloud failed for path '${path.getOrElse("")}'"
        )
        Future.failed(new Exception())
    }
  }

  private def upsertUpdatedWgsUbam(webClient: ClioWebClient,
                                   config: BaseArgs): Future[HttpResponse] = {
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

  private def prettyKey(config: BaseArgs): String = {
    s"FlowcellBarcode: ${config.flowcell.getOrElse("")}, LibraryName: ${config.libraryName
      .getOrElse("")}, " +
      s"Lane: ${config.lane.getOrElse("")}, Location: ${config.location.getOrElse("")}"
  }

  def ensureOkResponse(httpResponse: HttpResponse): HttpResponse = {
    if (httpResponse.status.isSuccess()) {
      httpResponse
    } else {
      logger.error(
        s"Got an error from the Clio server. Status code: ${httpResponse.status}"
      )
      throw new Exception()
    }
  }
}
