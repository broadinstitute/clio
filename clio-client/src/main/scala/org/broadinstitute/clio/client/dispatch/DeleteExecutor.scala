package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.HttpCredentials
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.{ClioCommand, DeleteCommand}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.generic.{
  CaseClassMapper,
  CaseClassTypeConverter
}
import org.broadinstitute.clio.util.model.{Location, UpsertId}

import scala.concurrent.{ExecutionContext, Future}

class DeleteExecutor[TI <: TransferIndex](deleteCommand: DeleteCommand[TI])
    extends Executor[UpsertId] {

  import deleteCommand.index.implicits._

  private val prettyKey = ClassUtil.formatFields(deleteCommand.key)
  val name: String = deleteCommand.index.name

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[UpsertId] = {
    if (!deleteCommand.key.location.equals(Location.GCP)) {
      Future.failed(
        new Exception(s"Only cloud ${name}s are supported at this time.")
      )
    } else {
      for {
        existingMetadata <- queryForKey(webClient)
          .logErrorMsg(
            s"Could not query the $name. No files have been deleted."
          )
        _ <- Future(checkPathConsistency(ioUtil, existingMetadata))
        upsertResponse <- deleteFiles(webClient, ioUtil, existingMetadata)
      } yield {
        upsertResponse
      }
    }
  }

  private def queryForKey(client: ClioWebClient)(
    implicit credentials: HttpCredentials,
    ec: ExecutionContext
  ): Future[deleteCommand.index.MetadataType] = {
    val keyToQueryMapper = CaseClassTypeConverter[
      deleteCommand.index.KeyType,
      deleteCommand.index.QueryInputType
    ](vals => vals.mapValues((v: Any) => Option(v)))

    val keyFields = new CaseClassMapper[deleteCommand.index.KeyType].names
    val outputToMetadataMapper = CaseClassTypeConverter[
      deleteCommand.index.QueryOutputType,
      deleteCommand.index.MetadataType
    ](inKeys => inKeys -- keyFields)

    val queryResponse = client
      .query(
        deleteCommand.index,
        keyToQueryMapper.convert(deleteCommand.key),
        includeDeleted = false
      )
      .logErrorMsg(s"There was a problem querying the Clio server for ${name}s")

    val queryOutputs = queryResponse
      .map(
        _.as[Seq[deleteCommand.index.QueryOutputType]].fold(throw _, identity)
      )

    queryOutputs.map { outputs =>
      val commandName = deleteCommand.index.commandName

      outputs.toList match {
        case Nil =>
          throw new Exception(
            s"No ${name}s were found for $prettyKey, nothing to delete."
          )
        case output :: Nil => outputToMetadataMapper.convert(output)
        case _ =>
          throw new Exception(
            s"${outputs.length} ${name}s were returned for $prettyKey, expected 1. Use ${ClioCommand.queryPrefix}$commandName to see what was returned."
          )
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

  private def deleteFiles(client: ClioWebClient,
                          ioUtil: IoUtil,
                          existingMetadata: deleteCommand.index.MetadataType)(
    implicit credentials: HttpCredentials,
    ec: ExecutionContext
  ): Future[UpsertId] = {
    val pathsToDelete = existingMetadata.pathsToDelete.filter { path =>
      val pathExists = ioUtil.googleObjectExists(path)
      if (!pathExists) {
        logger.warn(
          s"'$path' associated with $prettyKey does not exist in the cloud."
        )
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
        .upsert(deleteCommand.index, deleteCommand.key, markedAsDeleted)
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
