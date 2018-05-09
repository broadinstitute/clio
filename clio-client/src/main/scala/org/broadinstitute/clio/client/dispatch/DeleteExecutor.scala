package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.client.commands.DeleteCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.model.Location

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Executor for "delete" commands, which delete files in the cloud and
  * record the deletion in the clio-server.
  */
class DeleteExecutor[CI <: ClioIndex](deleteCommand: DeleteCommand[CI])(
  implicit ec: ExecutionContext
) extends Executor {

  private val prettyKey = ClassUtil.formatFields(deleteCommand.key)
  val name: String = deleteCommand.index.name

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    import Executor.SourceMonadOps

    for {
      existingMetadata <- checkPreconditions(webClient, ioUtil)
      pathsToDelete <- buildDelete(existingMetadata, ioUtil)
      _ <- ioUtil.deleteCloudObjects(pathsToDelete).mapError {
        case ex =>
          new RuntimeException(
            s"""Errors encountered while deleting files for $prettyKey.
               |The record for $prettyKey still exists in Clio.""".stripMargin,
            ex
          )
      }
      upsertId <- webClient
        .upsert(deleteCommand.index)(
          deleteCommand.key,
          existingMetadata.markDeleted(deleteCommand.note),
          // Always force because we're purposefully overwriting document status.
          force = true
        )
        .mapError {
          case ex =>
            new RuntimeException(
              s"""Failed to delete the $name record for $prettyKey in Clio.
                 |The associated files have been deleted in the cloud, but Clio doesn't know.
                 |Try re-running this command with '--force'.""".stripMargin,
              ex
            )
        }
    } yield {
      upsertId
    }
  }

  /**
    * Build a stream which, when pulled, will ensure that the command being executed
    * refers to a cloud document already registered in the clio-server, emitting the
    * existing metadata on success.
    */
  private def checkPreconditions(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[deleteCommand.index.MetadataType, NotUsed] = {
    if (!deleteCommand.key.location.equals(Location.GCP)) {
      Source.failed(
        new UnsupportedOperationException(
          s"Only cloud ${name}s are supported at this time."
        )
      )
    } else {
      val metadata: Source[deleteCommand.index.MetadataType, NotUsed] =
        webClient.getMetadataForKey(deleteCommand.index)(
          deleteCommand.key,
          includeDeleted = false
        )

      metadata.flatMapConcat { existing =>
        val nonCloudPaths = existing.pathsToDelete.foldLeft(Seq.empty[URI]) {
          (acc, uri) =>
            if (ioUtil.isGoogleObject(uri)) {
              acc
            } else {
              acc :+ uri
            }
        }

        if (nonCloudPaths.isEmpty) {
          Source.single(existing)
        } else {
          Source.failed(
            new IllegalStateException(
              s"""Inconsistent state detected, non-cloud paths registered to the $name for $prettyKey:
                 |${nonCloudPaths.mkString(",")}""".stripMargin
            )
          )
        }
      }.orElse {
        // Has to be lazy because `orElse` eagerly fails as soon as either branch fails.
        Source.lazily { () =>
          Source.failed(
            new IllegalStateException(
              s"No $name found in Clio for $prettyKey, nothing to delete."
            )
          )
        }
      }
    }
  }

  /**
    * Build a stream which, when pulled, will get the paths of all files-to-delete from
    * the given metadata, then for each path check:
    *
    * <ol>
    * <li>That the path points to cloud storage, and</li>
    * <li>That the file pointed to by the path actually exists.</li>
    * </ol>
    *
    * If all the checks pass, the stream will emit the list of paths-to-delete.
    * If not all checks pass, but the delete is being forced, the stream will emit the
    * list of only those paths which point to existing files.
    */
  private def buildDelete(
    metadata: deleteCommand.index.MetadataType,
    ioUtil: IoUtil
  ): Source[immutable.Iterable[URI], NotUsed] = {

    val paths = metadata.pathsToDelete.to[immutable.Iterable]
    Source[URI](paths)
      .mapAsync(paths.size) { path =>
        Future(path -> ioUtil.googleObjectExists(path))
      }
      .fold(Right(immutable.Seq.empty[URI]): Either[Seq[Throwable], immutable.Seq[URI]]) {
        case (acc, (uri, false)) =>
          val msg = s"'$uri' associated with $prettyKey does not exist in the cloud."
          val err = new IllegalStateException(msg)
          if (deleteCommand.force) {
            logger.warn(msg)
          }
          acc.fold(
            errs => Left(errs :+ err),
            uris => if (deleteCommand.force) Right(uris) else Left(Seq(err))
          )

        case (Right(uris), (uri, _)) => Right(uris :+ uri)
        case (Left(errs), _)         => Left(errs)
      }
      .flatMapConcat {
        _.fold(
          {
            case head +: tail =>
              val exception = head
              tail.foreach(exception.addSuppressed)
              Source.failed(exception)
          },
          paths => Source.single(paths)
        )
      }
  }
}
