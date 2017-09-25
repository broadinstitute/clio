package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands.DeleteWgsUbam
import org.broadinstitute.clio.client.util.{ClassUtil, IoUtil}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DeleteExecutorWgsUbam(deleteWgsUbam: DeleteWgsUbam) extends Executor {

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    if (!deleteWgsUbam.transferWgsUbamV1Key.location.equals(Location.GCP)) {
      Future
        .failed(new Exception("Only GCP wgs-ubams are supported at this time"))
    } else {
      for {
        queryResponses <- webClient
          .queryWgsUbam(
            TransferWgsUbamV1QueryInput(
              flowcellBarcode =
                Some(deleteWgsUbam.transferWgsUbamV1Key.flowcellBarcode),
              lane = Some(deleteWgsUbam.transferWgsUbamV1Key.lane),
              libraryName = Some(deleteWgsUbam.transferWgsUbamV1Key.libraryName),
              location = Some(deleteWgsUbam.transferWgsUbamV1Key.location),
              documentStatus = Some(DocumentStatus.Normal)
            ),
            includeDeleted = false
          )
          .map(webClient.ensureOkResponse) logErrorMsg "There was a problem querying the Clio server for wgs-ubams."
        wgsUbams <- webClient
          .unmarshal[Seq[TransferWgsUbamV1QueryOutput]](queryResponses)
          .logErrorMsg(
            "There was a problem unmarshalling the JSON response from Clio."
          )
        deleteResponses <- deleteWgsUbams(wgsUbams, webClient, ioUtil) logErrorMsg "There was an error while deleting some of all of the wgs-ubams."
      } yield {
        deleteResponses.size match {
          case 0 =>
            throw new Exception(
              "Deleted 0 wgs-ubams. None of the wgs-ubams queried were able to be deleted."
            )
          case s if s == wgsUbams.size =>
            logger.info(
              s"Successfully deleted ${deleteResponses.size} wgs-ubams."
            )
            queryResponses
          case _ =>
            throw new Exception(
              s"Deleted ${deleteResponses.size} wgs-ubams. " +
                s"Not all of the wgs-ubams queried for were able to be deleted! Check the log for details"
            )
        }
      }
    }
  }

  private def deleteWgsUbams(wgsUbams: Seq[TransferWgsUbamV1QueryOutput],
                             webClient: ClioWebClient,
                             ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[Seq[HttpResponse]] = {
    if (wgsUbams.isEmpty) {
      Future.failed(
        new Exception(
          s"No wgs-ubams were found for ${deleteWgsUbam.transferWgsUbamV1Key}. Nothing has been deleted."
        )
      )
    } else {
      // Futures are transformed into Future[Either] so that any errors don't cause the entire resulting
      // Future to be failed. Errors need to be preserved until all Futures have completed.
      val deleteFutures = wgsUbams.map(
        deleteWgsUbam(_, webClient, ioUtil)
          .transformWith {
            case Success(httpResponse) =>
              Future(webClient.ensureOkResponse(httpResponse))
            case Failure(ex) => Future.failed(ex)
          }
          .transformWith {
            case Success(httpResponse) => Future.successful(Right(httpResponse))
            case Failure(ex)           => Future.successful(Left(ex))
          }
      )
      Future.foldLeft(deleteFutures)(Seq.empty[HttpResponse])((acc, cur) => {
        cur match {
          case Left(_)             => acc
          case Right(httpResponse) => acc :+ httpResponse
        }
      })
    }
  }

  private def deleteWgsUbam(wgsUbam: TransferWgsUbamV1QueryOutput,
                            webClient: ClioWebClient,
                            ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {

    val key = TransferWgsUbamV1Key(
      wgsUbam.flowcellBarcode,
      wgsUbam.lane,
      wgsUbam.libraryName,
      wgsUbam.location
    )
    val prettyKey = ClassUtil.formatFields(key)

    def addNote(note: String): String = {
      wgsUbam.notes
        .map(existing => s"$existing\n$note")
        .getOrElse(note)
    }

    wgsUbam.ubamPath
      .map { ubamPath =>
        if (!ioUtil.isGoogleObject(ubamPath)) {
          Future.failed(
            new Exception(
              s"Inconsistent state detected: non-cloud path $ubamPath is registered to the wgs-ubam for $prettyKey"
            )
          )
        }

        if (ioUtil.googleObjectExists(ubamPath)) {
          logger.info(s"Deleting $ubamPath in the cloud.")
          if (ioUtil.deleteGoogleObject(ubamPath) == 0) {
            deleteInClio(key, addNote(deleteWgsUbam.note), webClient)
              .logErrorMsg(
                s"Failed to delete the wgs-ubam $prettyKey in Clio. " +
                  s"The file has been deleted in the cloud. " +
                  s"Clio now has a 'dangling pointer' to ${wgsUbam.ubamPath.getOrElse("")}. " +
                  s"Please try updating Clio by manually adding the wgs-ubam and setting the documentStatus to Deleted and making the ubamPath an empty String."
              )
          } else {
            Future.failed(
              new Exception(
                s"Failed to delete $ubamPath in the cloud. The wgs-ubam still exists in Clio and on cloud storage."
              )
            )
          }
        } else {
          logger.warn(
            s"$ubamPath associated with wgs-ubam for $prettyKey does not exist in the cloud. Deleting the record in Clio to reflect this."
          )
          deleteInClio(
            key,
            addNote(
              s"${deleteWgsUbam.note}\nNOTE: Path did not exist at time of deletion"
            ),
            webClient
          )
        }
      }
      .getOrElse {
        logger.warn(s"No path associated with wgs-ubam for $prettyKey.")
        deleteInClio(
          key,
          addNote(
            s"${deleteWgsUbam.note}\nNOTE: No path in metadata at time of deletion"
          ),
          webClient
        )
      }
  }

  private def deleteInClio(key: TransferWgsUbamV1Key,
                           notes: String,
                           webClient: ClioWebClient)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {

    val prettyKey = ClassUtil.formatFields(key)

    logger.info(s"Deleting wgs-ubam for $prettyKey in Clio.")
    webClient
      .addWgsUbam(
        key,
        TransferWgsUbamV1Metadata(
          documentStatus = Some(DocumentStatus.Deleted),
          notes = Some(notes)
        )
      )
      .map(webClient.ensureOkResponse)
  }
}
