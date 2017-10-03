package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpCredentials
import org.broadinstitute.clio.client.commands.DeleteGvcf
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryOutput
}
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DeleteExecutorGvcf(deleteGvcf: DeleteGvcf) extends Executor {

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {
    if (!deleteGvcf.transferGvcfV1Key.location.equals(Location.GCP)) {
      Future
        .failed(new Exception("Only GCP gvcfs are supported at this time"))
    } else {
      for {
        queryResponses <- webClient
          .query(
            GvcfIndex,
            deleteGvcf.transferGvcfV1Key,
            includeDeleted = false
          )
          .map(webClient.ensureOkResponse) logErrorMsg "There was a problem querying the Clio server for gvcfs."
        gvcf <- webClient
          .unmarshal[Seq[TransferGvcfV1QueryOutput]](queryResponses)
          .logErrorMsg(
            "There was a problem unmarshalling the JSON response from Clio."
          )
        deleteResponses <- deleteGvcfs(gvcf, webClient, ioUtil) logErrorMsg "There was an error while deleting some of all of the gvcfs."
      } yield {
        deleteResponses.size match {
          case 0 =>
            throw new Exception(
              "Deleted 0 gvcfs. None of the gvcfs queried were able to be deleted."
            )
          case s if s == gvcf.size =>
            logger.info(s"Successfully deleted $s Gvcfs.")
            queryResponses
          case _ =>
            throw new Exception(
              s"Deleted ${deleteResponses.size} gvcfs. " +
                s"Not all of the gvcfs queried for were able to be deleted! Check the log for details"
            )
        }
      }
    }
  }

  private def deleteGvcfs(gvcf: Seq[TransferGvcfV1QueryOutput],
                          webClient: ClioWebClient,
                          ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[Seq[HttpResponse]] = {
    if (gvcf.isEmpty) {
      Future.failed(
        new Exception(
          s"No gvcfs were found for ${deleteGvcf.transferGvcfV1Key}. Nothing has been deleted."
        )
      )
    } else {
      // Futures are transformed into Future[Either] so that any errors don't cause the entire resulting
      // Future to be failed. Errors need to be preserved until all Futures have completed.
      val deleteFutures = gvcf.map(
        deleteGvcf(_, webClient, ioUtil)
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

  private def deleteGvcf(gvcf: TransferGvcfV1QueryOutput,
                         webClient: ClioWebClient,
                         ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {

    val key = TransferGvcfV1Key(
      gvcf.location,
      gvcf.project,
      gvcf.sampleAlias,
      gvcf.version
    )
    val prettyKey = ClassUtil.formatFields(key)

    def addNote(note: String): String = {
      gvcf.notes
        .map(existing => s"$existing\n$note")
        .getOrElse(note)
    }

    gvcf.gvcfPath
      .map { gvcfPath =>
        if (!ioUtil.isGoogleObject(gvcfPath)) {
          Future.failed(
            new Exception(
              s"Inconsistent state detected: non-cloud path $gvcfPath is registered to the gvcf for $prettyKey."
            )
          )
        }

        if (ioUtil.googleObjectExists(gvcfPath)) {
          logger.info(s"Deleting $gvcfPath in the cloud.")
          if (ioUtil.deleteGoogleObject(gvcfPath) == 0) {
            deleteInClio(key, addNote(deleteGvcf.note), webClient).logErrorMsg(
              s"Failed to delete the gvcf for $prettyKey in Clio. " +
                s"The file has been deleted in the cloud. " +
                s"Clio now has a 'dangling pointer' to $gvcfPath. " +
                s"Please try updating Clio by manually adding the gvcf and setting the documentStatus to Deleted and making the gvcfPath an empty String."
            )
          } else {
            Future.failed(
              new Exception(
                s"Failed to delete $gvcfPath in the cloud. The gvcf still exists in Clio and on cloud storage."
              )
            )
          }
        } else {
          logger.warn(
            s"$gvcfPath associated with gvcf for $prettyKey does not exist in the cloud. Deleting the record in Clio to reflect this."
          )
          deleteInClio(
            key,
            addNote(
              s"${deleteGvcf.note}\nNOTE: Path did not exist at time of deletion"
            ),
            webClient
          )
        }
      }
      .getOrElse {
        logger.warn(s"No path associated with gvcf for $prettyKey.")
        deleteInClio(
          key,
          addNote(
            s"${deleteGvcf.note}\nNOTE: No path in metadata at time of deletion"
          ),
          webClient
        )
      }
  }

  private def deleteInClio(key: TransferGvcfV1Key,
                           notes: String,
                           webClient: ClioWebClient)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {

    val prettyKey = ClassUtil.formatFields(key)

    logger.info(s"Deleting gvcf for $prettyKey in Clio.")
    webClient
      .upsert(
        GvcfIndex,
        key,
        TransferGvcfV1Metadata(
          documentStatus = Some(DocumentStatus.Deleted),
          notes = Some(notes)
        )
      )
      .map(webClient.ensureOkResponse)
  }
}
