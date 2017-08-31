package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}
import org.broadinstitute.clio.client.util.WgsUbamUtil.TransferWgsUbamV1QueryOutputUtil

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DeleteWgsUbamCommand extends Command {

  override def execute(
    webClient: ClioWebClient,
    config: BaseArgs,
    ioUtil: IoUtil
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    if (!config.location.get.equals("GCP")) {
      return Future.failed(
        new Exception("Only GCP WgsUbams are supported at this time")
      )
    }
    for {
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
        .map(webClient.ensureOkResponse) logErrorMsg "There was a problem querying the Clio server for WgsUbams."
      wgsUbams <- webClient.unmarshal[Seq[TransferWgsUbamV1QueryOutput]](
        queryResponses
      )
      deleteResponses <- deleteWgsUbams(wgsUbams, webClient, config, ioUtil) logErrorMsg "There was an error while deleting some of all of the WgsUbams."
    } yield {
      deleteResponses.size match {
        case s if s == wgsUbams.size =>
          logger.info(s"Successfully deleted ${deleteResponses.size} WgsUbams.")
          deleteResponses.head
        case 0 =>
          throw new Exception(
            "Deleted 0 WgsUbams. None of the WgsUbams queried were able to be deleted."
          )
        case _ =>
          throw new Exception(
            s"Deleted ${deleteResponses.size} WgsUbams. " +
              s"Not all of the WgsUbams queried for were able to be deleted! Check the log for details"
          )
      }
    }
  }

  private def deleteWgsUbams(wgsUbams: Seq[TransferWgsUbamV1QueryOutput],
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
    // Futures are transformed into Future[Either] so that any errors don't cause the entire resulting
    // Future to be failed. Errors need to be preserved until all Futures have completed.
    val deleteFutures = wgsUbams.map(
      deleteWgsUbam(_, webClient, config, ioUtil)
        .logErrorMsg()
        .transformWith({
          case Success(httpResponse) => Future.successful(Right(httpResponse))
          case Failure(ex)           => Future.successful(Left(ex))
        })
    )
    Future.foldLeft(deleteFutures)(Seq.empty[HttpResponse])((acc, cur) => {
      cur match {
        case Left(_)             => acc
        case Right(httpResponse) => acc :+ httpResponse
      }
    })
  }

  private def deleteWgsUbam(wgsUbam: TransferWgsUbamV1QueryOutput,
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
      .map(webClient.ensureOkResponse) logErrorMsg (s"Failed to delete the WgsUbam ${wgsUbam
      .prettyKey()} in Clio. The file has been deleted in the cloud. " +
      s"Clio now has a 'dangling pointer' to ${wgsUbam.ubamPath.get}. " +
      s"Please try updating Clio by manually adding the WgsUbam and setting the documentStatus to Deleted and making the ubamPath an empty String.")
  }
}
