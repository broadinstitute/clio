package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.{LazyLogging, Logger}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.{FutureWithErrorMessage, IoUtil}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}
import org.broadinstitute.clio.client.util.WgsUbamUtil.TransferWgsUbamV1QueryOutputUtil

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DeleteWgsUbamCommand
    extends Command
    with LazyLogging
    with FailFastCirceSupport
    with ModelAutoDerivation
    with FutureWithErrorMessage {

  implicit val implicitLogger: Logger = logger

  override def execute(
    webClient: ClioWebClient,
    config: BaseArgs,
    ioUtil: IoUtil
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    for {
      _ <- webClient.verifyCloudPaths(config) withErrorMsg ()
      queryResponses <- webClient
        .queryWgsUbam(
          config.bearerToken.get,
          TransferWgsUbamV1QueryInput(
            flowcellBarcode = config.flowcell,
            lane = config.lane,
            libraryName = config.libraryName,
            location = Location.pathMatcher.get(config.location.get),
            documentStatus = Some(DocumentStatus.Normal)
          )
        )
        .map(webClient.ensureOkResponse) withErrorMsg "There was a problem querying the Clio server for WgsUbams."
      wgsUbams <- webClient.unmarshal[Seq[TransferWgsUbamV1QueryOutput]](
        queryResponses
      )
      deleteResponses <- deleteWgsUbams(wgsUbams, webClient, config, ioUtil)
    } yield {
      deleteResponses.size match {
        case s if s != deleteResponses.size =>
          throw new Exception(
            s"Deleted ${deleteResponses.size} WgsUbams. " +
              s"Not all of the WgsUbams queried for were able to be deleted! Check the log for details"
          )
        case 0 =>
          throw new Exception(
            "Deleted 0 WgsUbams. None of the WgsUbams queried were able to be deleted."
          )
        case _ =>
          logger.info(s"Successfully deleted ${deleteResponses.size} WgsUbams.")
          deleteResponses.head
      }
    }
  }

  def deleteWgsUbams(wgsUbams: Seq[TransferWgsUbamV1QueryOutput],
                     webClient: ClioWebClient,
                     config: BaseArgs,
                     ioUtil: IoUtil): Future[Seq[HttpResponse]] = {
    implicit val ec: ExecutionContext = webClient.executionContext
    if (wgsUbams.isEmpty) {
      return Future.failed(
        new Exception(
          s"No WgsUbams were found for $config. Nothing has been deleted."
        )
      )
    }
    Future.foldLeft(
      wgsUbams.map(deleteWgsUbam(_, webClient, config, ioUtil).transformWith({
        case Success(httpResponse) => Future.successful(Right(httpResponse))
        case Failure(ex)           => Future.successful(Left(ex))
      }))
    )(Seq.empty[HttpResponse])((acc, cur) => {
      cur match {
        case Left(_)             => acc
        case Right(httpResponse) => acc :+ httpResponse
      }
    })
  }

  def deleteWgsUbam(wgsUbam: TransferWgsUbamV1QueryOutput,
                    webClient: ClioWebClient,
                    config: BaseArgs,
                    ioUtil: IoUtil): Future[HttpResponse] = {
    implicit val ec: ExecutionContext = webClient.executionContext

    logger.info(s"Deleting ${wgsUbam.ubamPath.get} in the cloud.")
    if (ioUtil.googleObjectExists(wgsUbam.ubamPath.get)) {
      ioUtil.deleteGoogleObject(wgsUbam.ubamPath.get) match {
        case 0 => ()
        case 1 =>
          return Future
            .failed(
              new Exception(
                s"Failed to delete ${wgsUbam.ubamPath.get} in the cloud. The WgsUbam still exists in Clio and on cloud storage"
              )
            )
            .withErrorMsg()
      }
    } else {
      logger.warn(
        s"${wgsUbam.ubamPath.get} does not exist in the cloud. Deleting the WgsUbam in Clio to reflect this."
      )
    }

    logger.info(s"Deleting ${wgsUbam.prettyKey()} in Clio.")
    webClient
      .addWgsUbam(
        config.bearerToken.get,
        TransferWgsUbamV1Key(
          flowcellBarcode = wgsUbam.flowcellBarcode,
          lane = wgsUbam.lane,
          libraryName = wgsUbam.libraryName,
          location = wgsUbam.location
        ),
        TransferWgsUbamV1Metadata(
          documentStatus = Option(DocumentStatus.Deleted),
          notes = wgsUbam.notes
            .map(notes => s"$notes\n${config.notes.get}")
            .orElse(config.notes)
        )
      )
      .map(webClient.ensureOkResponse) withErrorMsg (s"Failed to delete the WgsUbam ${wgsUbam
      .prettyKey()} in Clio. The file has been deleted in the cloud. " +
      s"Clio now has a 'dangling pointer' to ${wgsUbam.ubamPath.get}. " +
      s"Please try updating Clio by manually adding the WgsUbam and setting the documentStatus to Deleted and making the ubamPath an empty String.")
  }
}
