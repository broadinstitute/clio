package org.broadinstitute.clio.client.dispatch

import java.io.IOException
import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.syntax.either._
import io.circe.Json
import org.broadinstitute.clio.client.commands.MoveCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{ClioIndex, Metadata}
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.immutable

/**
  * Executor for "move" commands, which move files in the cloud and record
  * the new paths in the clio-server.
  *
  * Delivery executors can extend this class to add custom IO operations which
  * should be performed when moving files into a FireCloud workspace.
  */
class MoveExecutor[CI <: ClioIndex](protected val moveCommand: MoveCommand[CI])(
  implicit ec: ExecutionContext
) extends Executor {

  import MoveExecutor._
  import moveCommand.index.implicits._

  private[dispatch] val name: String = moveCommand.index.name
  private[dispatch] val prettyKey = ClassUtil.formatFields(moveCommand.key)
  private val destination: URI = moveCommand.destination

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    import Executor.SourceMonadOps

    for {
      existingMetadata <- checkPreconditions(ioUtil, webClient)
      (movedMetadata, opsToPerform) <- buildMove(existingMetadata, ioUtil)
      if movedMetadata != existingMetadata
      _ <- runPreUpsertOps(opsToPerform, ioUtil).mapError {
        case ex =>
          new RuntimeException(
            s"""Errors encountered while copying files for $prettyKey.
               |Clio hasn't been updated, but some files may exist in two locations.""".stripMargin,
            ex
          )
      }
      upsertId <- webClient
        .upsert(moveCommand.index)(moveCommand.key, movedMetadata, force = true)
        .mapError {
          case ex =>
            new RuntimeException(
              s"""An error occurred while updating the $name record in Clio. All files associated with
                 |$prettyKey exist at both the old and new locations, but Clio only knows about the old
                 |location. Try re-running the command.""".stripMargin,
              ex
            )
        }
      _ <- ioUtil
        .deleteCloudObjects(opsToPerform.collect { case MoveOp(src, _) => src })
        .mapError {
          case ex =>
            new RuntimeException(
              s"""Errors encountered while deleting old files for $prettyKey.
                 |Please manually delete the old files.""".stripMargin,
              ex
            )
        }
    } yield {
      upsertId
    }
  }

  /**
    * Build a stream which, when pulled, will ensure that the command being executed
    * refers to a cloud document already registered in the clio-server, and that the
    * destination path points to a cloud directory, emitting the existing metadata on
    * success.
    */
  protected def checkPreconditions(
    ioUtil: IoUtil,
    webClient: ClioWebClient
  ): Source[moveCommand.index.MetadataType, NotUsed] = {
    if (moveCommand.key.location != Location.GCP) {
      Source.failed(
        new UnsupportedOperationException(
          s"Only cloud ${name}s are supported at this time."
        )
      )
    } else if (!ioUtil.isGoogleDirectory(destination)) {
      Source.failed(
        new IllegalArgumentException(
          s"The destination of the $name must be a cloud path ending with '/'."
        )
      )
    } else {
      webClient
        .getMetadataForKey(moveCommand.index)(
          moveCommand.key,
          includeDeleted = false
        )
        .orElse {
          // Has to be lazy because `orElse` eagerly fails as soon as either branch fails.
          Source.lazily { () =>
            Source.failed(
              new IllegalStateException(
                s"No $name found in Clio for $prettyKey, nothing to move."
              )
            )
          }
        }
    }
  }

  /**
    * Build a stream which, when pulled, will get the paths of all files-to-move from
    * the given metadata, checking:
    *
    * <ol>
    * <li>That there are some files registered to the document which can be moved, and</li>
    * <li>That all the paths refer to cloud files.</li>
    * </ol>
    *
    * If all the checks pass, the stream will emit a pair of:
    *
    * <ol>
    * <li>New metadata which should be upserted to the clio-server *after* IO is performed, and</li>
    * <li>A list of `IoOp`s representing the operations to perform as part of the move.</li>
    * </ol>
    *
    * Delivery subclasses can override this method to change the returned metadata / add
    * more IO operations.
    */
  protected[dispatch] def buildMove(
    metadata: moveCommand.index.MetadataType,
    ioUtil: IoUtil
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed] = {

    val preMovePaths = Metadata.extractPaths(metadata)

    if (preMovePaths.isEmpty) {
      Source.failed(
        new IllegalStateException(
          s"Nothing to move; no files registered to the $name for $prettyKey"
        )
      )
    } else {
      val movedMetadata = metadata.moveInto(destination, moveCommand.newBasename)
      val postMovePaths = Metadata.extractPaths(movedMetadata)

      val opsToPerform = preMovePaths.flatMap {
        case (fieldName, path) => {
          /*
           * Assumptions:
           *   1. If the field exists pre-move, it will still exist post-move
           *   2. If the field is a URI pre-move, it will still be a URI post-move
           */
          Some(MoveOp(path, postMovePaths(fieldName)))
            .filterNot(op => op.src.equals(op.dest))
        }
      }

      Source(opsToPerform)
        .fold(Seq.empty[URI]) {
          case (acc, op) =>
            if (ioUtil.isGoogleObject(op.src)) {
              acc
            } else {
              acc :+ op.src
            }
        }
        .flatMapConcat { nonCloudPaths =>
          if (nonCloudPaths.isEmpty) {
            if (opsToPerform.isEmpty) {
              logger.info(
                s"Nothing to move; all files already exist at $destination and no other metadata changes need applied."
              )
              Source.empty
            } else {
              Source.single(movedMetadata -> opsToPerform.to[immutable.Seq])
            }
          } else {
            Source.failed(
              new IllegalStateException(
                s"""Inconsistent state detected, non-cloud paths registered to the $name for $prettyKey:
                   |${nonCloudPaths.mkString(",")}""".stripMargin
              )
            )
          }
        }
    }
  }
}

object MoveExecutor {

  /**
    * Description of an IO operation to be performed as part of moving
    * files for a document in Clio.
    */
  sealed trait IoOp

  /**
    * Case for files which should be copied before updating the clio-server,
    * with the old copy deleted after a successful upsert.
    */
  case class MoveOp(src: URI, dest: URI) extends IoOp

  /**
    * Case for files which should be copied, with the old copy left on disk
    * after upsert-ing to the server.
    */
  case class CopyOp(src: URI, dest: URI) extends IoOp

  /**
    * Case for new files which should be generated by writing metadata
    * contents to disk.
    */
  case class WriteOp(contents: String, dest: URI) extends IoOp

  /**
    * Build a stream which, when pulled, will run the pre-upsert component of each of the
    * given IO operations. If all operations succeed, the stream will emit (). Otherwise,
    * the stream will fail with an error indicating all IO failures.
    */
  private[dispatch] def runPreUpsertOps(
    ops: immutable.Seq[IoOp],
    ioUtil: IoUtil
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    Source(ops)
      .mapAsyncUnordered(ops.size + 1) { op =>
        Future {
          Either.catchNonFatal {
            op match {
              case MoveOp(src, dest)       => ioUtil.copyGoogleObject(src, dest)
              case CopyOp(src, dest)       => ioUtil.copyGoogleObject(src, dest)
              case WriteOp(contents, dest) => ioUtil.writeGoogleObjectData(contents, dest)
            }
          }
        }
      }
      .fold(Seq.empty[Throwable]) { (acc, attempt) =>
        attempt.fold(acc :+ _, _ => acc)
      }
      .flatMapConcat {
        case Seq() => Source.single(())
        case head +: tail =>
          val exception =
            new IOException("Failed to perform pre-upsert IO operations", head)
          tail.foreach(exception.addSuppressed)
          Source.failed(exception)
      }
  }
}
