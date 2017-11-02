package org.broadinstitute.clio.client.dispatch

import java.net.URI

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
  private val destination: URI = moveCommand.destination
  private val name: String = moveCommand.index.name

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {
    for {
      _ <- Future(verifyCloudPaths(ioUtil))
      existingMetadata <- queryForKey(webClient)
        .logErrorMsg(s"Could not query the $name. No files have been moved.")
      upsertResponse <- moveFiles(webClient, ioUtil, existingMetadata)
    } yield {
      upsertResponse
    }
  }

  private def verifyCloudPaths(ioUtil: IoUtil): Unit = {
    if (moveCommand.key.location != Location.GCP) {
      throw new IllegalArgumentException(
        s"Only cloud ${name}s are supported at this time."
      )
    }

    if (!ioUtil.isGoogleDirectory(destination)) {
      throw new IllegalArgumentException(
        s"The destination of the $name must be a cloud path ending with '/'."
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

  /**
    * Here there be hacks to reduce boilerplate; may the typed gods have mercy.
    *
    * We flatten out the metadata instances for pre- and post-move into Maps
    * from string keys to Any values, then flatMap away Options by extracting
    * the underlying values from Somes and removing Nones.
    *
    * The Option-removal is needed so we can successfully pattern-match on URI
    * later when building up the list of (preMove -> postMove) paths. Without it,
    * matching on Option[URI] fails because of type erasure.
    */
  private def flattenMetadata(
    metadata: moveCommand.index.MetadataType
  ): Map[String, Any] = {
    val metadataMapper = new CaseClassMapper[moveCommand.index.MetadataType]
    metadataMapper.vals(metadata).flatMap {
      case (key, optValue: Option[_]) => optValue.map(v => key -> v)
      case (key, nonOptValue)         => Some(key -> nonOptValue)
    }
  }

  private def moveFiles(client: ClioWebClient,
                        ioUtil: IoUtil,
                        existingMetadata: moveCommand.index.MetadataType)(
    implicit credentials: HttpCredentials,
    ec: ExecutionContext
  ): Future[HttpResponse] = {

    val newMetadata = existingMetadata.moveInto(destination)
    val preMoveFields = flattenMetadata(existingMetadata)
    val postMoveFields = flattenMetadata(newMetadata)

    val movesToPerform = preMoveFields.flatMap {
      case (fieldName, path: URI) => {
        /*
         * Assumptions:
         *   1. If the field exists pre-move, it will still exist post-move
         *   2. If the field is a URI pre-move, it will still be a URI post-move
         */
        Some(path -> postMoveFields(fieldName).asInstanceOf[URI])
          .filterNot { case (oldPath, newPath) => oldPath.equals(newPath) }
      }
      case _ => None
    }

    if (movesToPerform.isEmpty) {
      logger.warn("Nothing to move.")
      Future.successful(HttpResponse(StatusCodes.OK))
    } else {
      val oldPaths = movesToPerform.keys
      oldPaths.foreach { path =>
        if (!ioUtil.isGoogleObject(path)) {
          sys.error(
            s"Inconsistent state detected: non-cloud path '$path' is registered to the $name for $prettyKey"
          )
        }
      }

      val sourcesAsString = movesToPerform.keys.mkString("'", "', '", "'")

      val googleCopies = movesToPerform.map {
        case (oldPath, newPath) =>
          Future(copyGoogleObject(oldPath, newPath, ioUtil))
      }

      // VERY IMPORTANT that this is lazy; we cannot delete anything
      // until Clio has been updated successfully.
      lazy val googleDeletes = oldPaths.map { oldPath =>
        Future(deleteGoogleObject(oldPath, ioUtil))
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

  private def copyGoogleObject(source: URI,
                               destination: URI,
                               ioUtil: IoUtil): Unit = {
    if (ioUtil.copyGoogleObject(source, destination) != 0) {
      throw new Exception(
        s"Copy files in the cloud failed from '$source' to '$destination'"
      )
    }
  }

  private def deleteGoogleObject(path: URI, ioUtil: IoUtil): Unit = {
    if (ioUtil.deleteGoogleObject(path) != 0) {
      throw new Exception(s"Deleting file in the cloud failed for path '$path'")
    }
  }
}
