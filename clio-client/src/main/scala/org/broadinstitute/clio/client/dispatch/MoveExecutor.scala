package org.broadinstitute.clio.client.dispatch

import java.net.URI

import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.MoveCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.broadinstitute.clio.util.model.{Location, UpsertId}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class MoveExecutor[TI <: TransferIndex](moveCommand: MoveCommand[TI])
    extends Executor[Option[UpsertId]] {

  import moveCommand.index.implicits._

  private[dispatch] val name: String = moveCommand.index.name
  private[dispatch] val prettyKey = ClassUtil.formatFields(moveCommand.key)
  private val destination: URI = moveCommand.destination

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[Option[UpsertId]] = {
    for {
      _ <- Future(verifyCloudPaths(ioUtil))
      existingMetadata <- webClient
        .getMetadataForKey(moveCommand.index)(moveCommand.key)
        .map {
          _.getOrElse(
            throw new IllegalStateException(
              s"No $name found in Clio for $prettyKey, nothing to move."
            )
          )
        }
      upsertResponse <- moveFiles(webClient, ioUtil, existingMetadata)
    } yield {
      upsertResponse
    }
  }

  private def verifyCloudPaths(ioUtil: IoUtil): Unit = {
    if (moveCommand.key.location != Location.GCP) {
      throw new UnsupportedOperationException(
        s"Only cloud ${name}s are supported at this time."
      )
    }

    if (!ioUtil.isGoogleDirectory(destination)) {
      throw new IllegalArgumentException(
        s"The destination of the $name must be a cloud path ending with '/'."
      )
    }
  }

  /**
    * Given one of our `TransferMetadata` classes, extract out all fields that
    * store path-related information into a map from 'fieldName' -> 'path'.
    *
    * Used to build a generic before-and-after move comparison to determine
    * which paths in a metadata object will actually be affected by the move.
    */
  private def extractPaths(
    metadata: moveCommand.index.MetadataType
  ): Map[String, URI] = {
    val metadataMapper = new CaseClassMapper[moveCommand.index.MetadataType]
    /*
     * Here there be hacks to reduce boilerplate; may the typed gods have mercy.
     *
     * We flatten out the metadata instances for pre- and post-move into Maps
     * from string keys to Any values, then flatMap away Options by extracting
     * the underlying values from Somes and removing Nones, then finally filter
     * away any non-URI values.
     *
     * The Option-removal is needed so we can successfully pattern-match on URI
     * later when building up the list of (preMove -> postMove) paths. Without it,
     * matching on Option[URI] fails because of type erasure.
     */
    metadataMapper
      .vals(metadata)
      .flatMap {
        case (key, optValue: Option[_]) => optValue.map(v => key -> v)
        case (key, nonOptValue)         => Some(key -> nonOptValue)
      }
      .flatMap {
        case (key, value: URI) => Some(key -> value)
        case _                 => None
      }
  }

  private def moveFiles(
    client: ClioWebClient,
    ioUtil: IoUtil,
    existingMetadata: moveCommand.index.MetadataType
  )(implicit ec: ExecutionContext): Future[Option[UpsertId]] = {

    val newMetadata =
      existingMetadata.moveInto(destination, moveCommand.newBasename)
    val preMovePaths = extractPaths(existingMetadata)
    val postMovePaths = extractPaths(newMetadata)

    val movesToPerform = preMovePaths.flatMap {
      case (fieldName, path) => {
        /*
         * Assumptions:
         *   1. If the field exists pre-move, it will still exist post-move
         *   2. If the field is a URI pre-move, it will still be a URI post-move
         */
        Some(path -> postMovePaths(fieldName)).filterNot {
          case (oldPath, newPath) => oldPath.equals(newPath)
        }
      }
    }

    if (preMovePaths.isEmpty) {
      Future.failed(
        new IllegalStateException(
          s"Nothing to move; no files registered to the $name for $prettyKey"
        )
      )
    } else if (movesToPerform.isEmpty) {
      logger.info(s"Nothing to move; all files already exist at $destination")
      Future.successful(None)
    } else {
      val oldPaths = movesToPerform.keys
      val pathCheck = Future {
        oldPaths.foreach { path =>
          if (!ioUtil.isGoogleObject(path)) {
            throw new IllegalStateException(
              s"Inconsistent state detected: non-cloud path '$path' is registered to the $name for $prettyKey"
            )
          }
        }
      }

      lazy val googleCopies = movesToPerform.map {
        case (oldPath, newPath) =>
          Future(copyGoogleObject(oldPath, newPath, ioUtil))
      }

      // VERY IMPORTANT that this is lazy; we cannot delete anything
      // until Clio has been updated successfully.
      lazy val googleDeletes: Iterable[Future[Either[URI, Unit]]] = oldPaths.map {
        oldPath =>
          Future(deleteGoogleObject(oldPath, ioUtil)).transformWith {
            case Success(_) =>
              Future.successful(Right(()))
            case Failure(ex) =>
              logger.error(s"Failed to delete ${oldPath.toString}", ex)
              Future.successful(Left(oldPath))
          }
      }

      for {
        _ <- pathCheck
        _ <- Future
          .sequence(googleCopies)
          .recover {
            case ex =>
              throw new RuntimeException(
                s"""An error occurred while copying files in the cloud. Clio hasn't been updated,
                   |but some files for $prettyKey may exist in two locations. Check the logs to see
                   |which copy commands failed, and why, then try re-running this command. If this
                   |can't be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}.""".stripMargin,
                ex
              )
          }
        upsertResponse <- client
          .upsert(moveCommand.index)(moveCommand.key, newMetadata)
          .recover {
            case ex =>
              throw new RuntimeException(
                s"""An error occurred while updating the $name record in Clio. All files associated with
                   |$prettyKey exist at both the old and new locations, but Clio only knows about the old
                   |location. Try removing the file(s) at $destination and re-running this command.
                   |If this can't be done, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}""".stripMargin,
                ex
              )
          }
        _ <- Future
          .sequence(googleDeletes)
          .map((moves: Iterable[Either[URI, Unit]]) => {
            if (moves.forall(_.isRight)) {
              logger.info("All objects were successfully deleted in gcloud.")
            } else {
              val notDeleted = moves.filter(_.isLeft)
              throw new RuntimeException(
                s"""The old files associated with $prettyKey were not able to be deleted.
                 |Please manually delete the old files at:
                 |${notDeleted.mkString(",\n")}
                 |If you cannot delete the files, please contact the Green Team at ${ClioClientConfig.greenTeamEmail}""".stripMargin
              )
            }
          })
      } yield {
        val sourcesAsString = movesToPerform.keys.mkString("'", "', '", "'")
        logger.info(s"Successfully moved $sourcesAsString to '$destination'")
        Some(upsertResponse)
      }
    }
  }

  private def copyGoogleObject(source: URI, destination: URI, ioUtil: IoUtil): Unit = {
    if (ioUtil.copyGoogleObject(source, destination) != 0) {
      throw new RuntimeException(
        s"Copy files in the cloud failed from '$source' to '$destination'"
      )
    }
  }

  private def deleteGoogleObject(path: URI, ioUtil: IoUtil): Unit = {
    if (ioUtil.deleteGoogleObject(path) != 0) {
      throw new RuntimeException(s"Deleting file in the cloud failed for path '$path'")
    }
  }
}
