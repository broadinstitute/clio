package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.commands.DeleteGvcf
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.util.GvcfUtil.TransferGvcfV1QueryOutputUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput,
  TransferGvcfV1QueryOutput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DeleteExecutorGvcf(deleteGvcf: DeleteGvcf) extends Executor {

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    if (!deleteGvcf.transferGvcfV1Key.location.equals(Location.GCP)) {
      Future
        .failed(new Exception("Only GCP Gvcfs are supported at this time"))
    } else {
      for {
        queryResponses <- webClient
          .queryGvcf(
            TransferGvcfV1QueryInput(
              documentStatus = Some(DocumentStatus.Normal),
              location = Some(deleteGvcf.transferGvcfV1Key.location),
              project = Some(deleteGvcf.transferGvcfV1Key.project),
              sampleAlias = Some(deleteGvcf.transferGvcfV1Key.sampleAlias),
              version = Some(deleteGvcf.transferGvcfV1Key.version)
            ),
            includeDeleted = false
          )
          .map(webClient.ensureOkResponse) logErrorMsg "There was a problem querying the Clio server for Gvcfs."
        gvcf <- webClient
          .unmarshal[Seq[TransferGvcfV1QueryOutput]](queryResponses)
          .logErrorMsg(
            "There was a problem unmarshalling the JSON response from Clio."
          )
        deleteResponses <- deleteGvcfs(gvcf, webClient, ioUtil) logErrorMsg "There was an error while deleting some of all of the Gvcfs."
      } yield {
        deleteResponses.size match {
          case 0 =>
            throw new Exception(
              "Deleted 0 Gvcfs. None of the Gvcfs queried were able to be deleted."
            )
          case s if s == gvcf.size =>
            logger.info(s"Successfully deleted ${s} Gvcfs.")
            queryResponses
          case _ =>
            throw new Exception(
              s"Deleted ${deleteResponses.size} Gvcfs. " +
                s"Not all of the Gvcfs queried for were able to be deleted! Check the log for details"
            )
        }
      }
    }
  }

  private def deleteGvcfs(gvcf: Seq[TransferGvcfV1QueryOutput],
                          webClient: ClioWebClient,
                          ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[Seq[HttpResponse]] = {
    if (gvcf.isEmpty) {
      Future.failed(
        new Exception(
          s"No Gvcfs were found for ${deleteGvcf.transferGvcfV1Key}. Nothing has been deleted."
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
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {

    val gvcfPath: String = gvcf.gvcfPath.getOrElse("")

    def deleteInClio(): Future[HttpResponse] = {
      logger.info(s"Deleting ${gvcf.prettyKey()} in Clio.")
      webClient
        .addGvcf(
          TransferGvcfV1Key(
            location = gvcf.location,
            project = gvcf.project,
            sampleAlias = gvcf.sampleAlias,
            version = gvcf.version
          ),
          TransferGvcfV1Metadata(
            documentStatus = Option(DocumentStatus.Deleted),
            notes = gvcf.notes
              .map(notes => s"$notes\n${deleteGvcf.note}")
              .orElse(Some(deleteGvcf.note))
          )
        )
        .map(webClient.ensureOkResponse)
        .logErrorMsg(
          s"Failed to delete the Gvcf ${gvcf.prettyKey()} in Clio. " +
            s"The file has been deleted in the cloud. " +
            s"Clio now has a 'dangling pointer' to $gvcfPath. " +
            s"Please try updating Clio by manually adding the Gvcf and setting the documentStatus to Deleted and making the gvcfPath an empty String."
        )
    }

    logger.info(s"Deleting $gvcfPath in the cloud.")
    if (ioUtil.googleObjectExists(gvcfPath)) {
      if (ioUtil.deleteGoogleObject(gvcfPath) == 0) {
        deleteInClio()
      } else {
        Future.failed(
          new Exception(
            s"Failed to delete $gvcfPath in the cloud. The Gvcf still exists in Clio and on cloud storage"
          )
        )
      }
    } else {
      logger.warn(
        s"$gvcfPath does not exist in the cloud. Deleting the Gvcf in Clio to reflect this."
      )
      deleteInClio()
    }
  }
}
