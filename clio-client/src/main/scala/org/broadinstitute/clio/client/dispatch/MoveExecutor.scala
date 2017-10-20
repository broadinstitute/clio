package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.HttpCredentials
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.{ClioCommand, MoveCommand}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.generic.{
  CaseClassMapper,
  CaseClassTypeConverter
}
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.{ExecutionContext, Future}

class MoveExecutor[TI <: TransferIndex](moveCommand: MoveCommand[TI])
    extends Executor {

  import moveCommand.index.implicits._

  private val prettyKey = ClassUtil.formatFields(moveCommand.key)
  private val destination: String = moveCommand.destination
  private val name: String = moveCommand.index.name

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {
    for {
      _ <- Future(verifyCloudPaths(ioUtil))
      existingMetadata <- queryForKey(webClient)
        .logErrorMsg(s"Could not query the $name. No files have been moved.")
      _ <- Future(checkDestinationVsExistingPaths(ioUtil, existingMetadata))
      upsertResponse <- moveFiles(webClient, ioUtil, existingMetadata)
    } yield {
      upsertResponse
    }
  }

  private def verifyCloudPaths(ioUtil: IoUtil): Unit = {
    if (moveCommand.key.location != Location.GCP) {
      throw new Exception(s"Only cloud ${name}s are supported at this time.")
    }

    if (!ioUtil.isGoogleObject(destination)) {
      throw new Exception(
        s"The destination of the $name must be a cloud path. '$destination' is not a cloud path."
      )
    }
  }

  private def queryForKey(client: ClioWebClient)(
    implicit credentials: HttpCredentials,
    ec: ExecutionContext
  ): Future[moveCommand.index.MetadataType] = {

    val keyToQueryMapper = CaseClassTypeConverter[
      moveCommand.index.KeyType,
      moveCommand.index.QueryInputType
    ](vals => vals.mapValues((v: Any) => Option(v)))

    val keyFields = new CaseClassMapper[moveCommand.index.KeyType].names
    val outputToMetadataMapper = CaseClassTypeConverter[
      moveCommand.index.QueryOutputType,
      moveCommand.index.MetadataType
    ](inKeys => inKeys -- keyFields)

    val queryResponse = client
      .query(
        moveCommand.index,
        keyToQueryMapper.convert(moveCommand.key),
        includeDeleted = false
      )
      .logErrorMsg("There was an error contacting the Clio server.")

    val queryOutputs = queryResponse
      .flatMap(client.unmarshal[Seq[moveCommand.index.QueryOutputType]])

    queryOutputs.map { outputs =>
      val commandName = moveCommand.index.commandName

      outputs.toList match {
        case Nil =>
          throw new Exception(
            s"No ${name}s were found for $prettyKey. Use ${ClioCommand.addPrefix}$commandName to add a $name."
          )
        case output :: Nil => outputToMetadataMapper.convert(output)
        case _ =>
          throw new Exception(
            s"${outputs.length} ${name}s were returned for $prettyKey, expected 1. Use ${ClioCommand.queryPrefix}$commandName to see what was returned."
          )
      }
    }
  }

  private def checkDestinationVsExistingPaths(
    ioUtil: IoUtil,
    metadata: moveCommand.index.MetadataType
  ): Unit = {
    val pathsToMove = metadata.pathsToMove

    pathsToMove.toList match {
      case Nil =>
        throw new Exception(
          s"The $name for $prettyKey has no registered paths, and can't be moved."
        )
      case path :: Nil => {
        if (!ioUtil.isGoogleObject(path)) {
          throw new Exception(
            s"Inconsistent state detected: non-cloud path '$path' is registered to the $name for $prettyKey."
          )
        }
      }
      case _ => {
        if (!ioUtil.isGoogleDirectory(destination)) {
          throw new Exception(
            s"""The $name for $prettyKey is associated with ${pathsToMove.length} files, so the move destination must be a directory.
               |Signal that you're OK with this by ending the destination argument with '/'""".stripMargin
          )
        }
      }
    }
  }

  private def moveFiles(client: ClioWebClient,
                        ioUtil: IoUtil,
                        existingMetadata: moveCommand.index.MetadataType)(
    implicit credentials: HttpCredentials,
    ec: ExecutionContext
  ): Future[HttpResponse] = {

    val preMovePaths = existingMetadata.pathsToMove
    val newMetadata = existingMetadata.moveInto(destination)
    val postMovePaths = newMetadata.pathsToMove

    val pathsToMove =
      preMovePaths.filterNot(path => postMovePaths.contains(path))

    if (pathsToMove.isEmpty) {
      logger.warn("Nothing to move.")
      Future.successful(HttpResponse(StatusCodes.OK))
    } else {
      val sourcesAsString =
        pathsToMove.mkString("'", "', '", "'")

      val googleCopies = pathsToMove.map { p =>
        Future(copyGoogleObject(p, destination, ioUtil))
      }

      // VERY IMPORTANT that this is lazy; we cannot delete anything
      // until Clio has been updated successfully.
      lazy val googleDeletes = pathsToMove.map { p =>
        Future(deleteGoogleObject(p, ioUtil))
      }

      for {
        _ <- Future
          .sequence(googleCopies)
          .logErrorMsg(
            s"""An error occurred while copying files in the cloud. Clio hasn't been updated,
               |but some files for $prettyKey may exist in two locations. Check the logs to see
               |which copy commands failed, and why, then try re-running this command. If this
               |can't be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}.""".stripMargin
          )
        upsertResponse <- client
          .upsert(moveCommand.index, moveCommand.key, newMetadata)
          .logErrorMsg(
            s"""An error occurred while updating the $name record in Clio. All files associated with
               |$prettyKey exist at both the old and new locations, but Clio only knows about the old
               |location. Try removing the file(s) at $destination and re-running this command.
               |If this can't be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}""".stripMargin
          )
        _ <- Future
          .sequence(googleDeletes)
          .logErrorMsg(
            s"""The old files associated with $prettyKey were not able to be deleted.
               |Please manually delete the old files at: $sourcesAsString.
               |If this can't be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}""".stripMargin
          )
      } yield {
        logger.info(s"Successfully moved $sourcesAsString to '$destination'")
        upsertResponse
      }
    }
  }

  private def copyGoogleObject(source: String,
                               destination: String,
                               ioUtil: IoUtil): Unit = {
    if (ioUtil.copyGoogleObject(source, destination) != 0) {
      throw new Exception(
        s"Copy files in the cloud failed from '$source' to '$destination'"
      )
    }
  }

  private def deleteGoogleObject(path: String, ioUtil: IoUtil): Unit = {
    if (ioUtil.deleteGoogleObject(path) != 0) {
      throw new Exception(s"Deleting file in the cloud failed for path '$path'")
    }
  }
}
