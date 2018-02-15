package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.DeleteCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.model.{Location, UpsertId}

import scala.concurrent.{ExecutionContext, Future}

class DeleteExecutor[TI <: TransferIndex](deleteCommand: DeleteCommand[TI])
    extends Executor[UpsertId] {

  private val prettyKey = ClassUtil.formatFields(deleteCommand.key)
  val name: String = deleteCommand.index.name

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[UpsertId] = {
    if (!deleteCommand.key.location.equals(Location.GCP)) {
      Future.failed(
        new Exception(s"Only cloud ${name}s are supported at this time.")
      )
    } else {
      for {
        existingMetadata <- webClient
          .getMetadataForKey(deleteCommand.index)(deleteCommand.key)
          .map {
            _.getOrElse(
              throw new RuntimeException(
                s"No ${name} found in Clio for $prettyKey, nothing to delete."
              )
            )
          }
        _ <- Future(checkPathConsistency(ioUtil, existingMetadata))
        upsertResponse <- deleteFiles(webClient, ioUtil, existingMetadata)
      } yield {
        upsertResponse
      }
    }
  }

  private def checkPathConsistency(
    ioUtil: IoUtil,
    metadata: deleteCommand.index.MetadataType
  ): Unit = {
    metadata.pathsToDelete.foreach { path =>
      if (!ioUtil.isGoogleObject(path)) {
        throw new Exception(
          s"Inconsistent state detected: non-cloud path '$path' is registered to the cloud $name for $prettyKey"
        )
      }
    }
  }

  private def deleteFiles(
    client: ClioWebClient,
    ioUtil: IoUtil,
    existingMetadata: deleteCommand.index.MetadataType
  )(implicit ec: ExecutionContext): Future[UpsertId] = {
    val pathsToDelete = existingMetadata.pathsToDelete.filter { path =>
      val pathExists = ioUtil.googleObjectExists(path)
      val err = s"'$path' associated with $prettyKey does not exist in the cloud."
      if (!pathExists) {
        if (!deleteCommand.force) {
          throw new IllegalStateException(
            err + " Use --force to mark the record as deleted in Clio"
          )
        }
        logger.warn(err)
      }
      pathExists
    }

    val googleDeletes = pathsToDelete.map { path =>
      Future {
        logger.info(s"Deleting '$path' in the cloud.")
        if (ioUtil.deleteGoogleObject(path) != 0) {
          // We log immediately here because we Future.sequence these together later,
          // and that operation short-circuits and returns the first error it hits, but
          // we want messages for all failed deletes to appear in the logs.
          val err = s"Could not delete '$path' in the cloud."
          logger.error(err)
          throw new Exception(err)
        }
      }
    }

    val markedAsDeleted = existingMetadata.markDeleted(deleteCommand.note)

    for {
      _ <- Future
        .sequence(googleDeletes)
        .logErrorMsg(
          s"""Failed to delete at least one file associated with $prettyKey on the cloud.
             |The record for $prettyKey still exists in Clio. Check the logs to see which
             |delete commands failed, and why, then try re-running this command. If this
             |can't be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}""".stripMargin
        )
      upsertResponse <- client
        .upsert(deleteCommand.index)(deleteCommand.key, markedAsDeleted)
        .logErrorMsg(
          s"""Failed to delete the $name record for $prettyKey in Clio.
             |The associated files have been deleted in the cloud, but Clio doesn't know.
             |Check the logs to see what error occurred, and try re-running this command.
             |If this can't be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}""".stripMargin
        )
    } yield {
      logger.info(
        s"Successfully deleted ${pathsToDelete.mkString("'", "', '", "'")}"
      )
      upsertResponse
    }
  }
}
