package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.client.commands.MoveCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext
import scala.collection.immutable

class MoveExecutor[CI <: ClioIndex](protected val moveCommand: MoveCommand[CI])(
  implicit ec: ExecutionContext
) extends Executor {

  import MoveExecutor.MoveOp
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
      existingMetadata <- verifyArgs(ioUtil, webClient)
      (movedMetadata, opsToPerform) <- buildMove(existingMetadata, ioUtil).orElse {
        Source.single(existingMetadata -> immutable.Seq.empty)
      }
      if movedMetadata != existingMetadata
      _ <- ioUtil.copyCloudObjects(opsToPerform.map(op => op.src -> op.dest)).mapError {
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
        .deleteCloudObjects(opsToPerform.collect { case op if op.deleteSrc => op.src })
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

  private def verifyArgs(
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

  protected def buildMove(
    metadata: moveCommand.index.MetadataType,
    ioUtil: IoUtil
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[MoveOp]), NotUsed] = {

    val preMovePaths = extractPaths(metadata)

    if (preMovePaths.isEmpty) {
      Source.failed(
        new IllegalStateException(
          s"Nothing to move; no files registered to the $name for $prettyKey"
        )
      )
    } else {
      val movedMetadata = metadata.moveInto(destination, moveCommand.newBasename)
      val postMovePaths = extractPaths(movedMetadata)

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

  /**
    * Given one of our `Metadata` classes, extract out all fields that
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
      .collect {
        /*
         * This filters out non-`URI` values in a way that allows the compiler
         * to change the map's type vars from `[String, Any]` to `[String, URI]`.
         *
         * Using `filter` with `inInstanceOf[URI]` would achieve the same filtering,
         * but it'd leave the resulting type vars as `[String, Any]`.
         */
        case (key, value: URI) => key -> value
      }
  }
}

object MoveExecutor {
  case class MoveOp(src: URI, dest: URI, deleteSrc: Boolean = true)
}
