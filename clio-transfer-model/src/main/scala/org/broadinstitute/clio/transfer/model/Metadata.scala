package org.broadinstitute.clio.transfer.model

import java.io.File
import java.net.URI

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.broadinstitute.clio.util.model.DocumentStatus

import scala.reflect.ClassTag

/**
  * Common information for metadata in all clio indexes.
  */
trait Metadata[M <: Metadata[M]] { self: M =>

  /**
    * Status of the document represented by this metadata,
    * primarily used to flag deletion.
    */
  val documentStatus: Option[DocumentStatus]

  /**
    * Notes used to track changes to metadata (primarily deletions).
    */
  val notes: Option[String]

  /**
    * Paths to all files that should be deleted by the clio-client delete
    * command for this metadata's index.
    */
  def pathsToDelete: Seq[URI]

  /**
    * Return a copy of this object in which all files have been moved
    * to the given `destination` directory, optionally changing their
    * base-names in the process.
    *
    * Until we move to a Path-based API, `destination` must end with '/'.
    */
  def moveInto(destination: URI, newBasename: Option[String] = None): M = {
    if (!destination.getPath.endsWith("/")) {
      sys.error(
        s"Non-directory destination '$destination' given for metadata move"
      )
    }

    mapMove {
      case (pathOpt, ext) =>
        pathOpt.map(Metadata.buildFilePath(_, destination, ext, newBasename))
    }
  }

  /**
    * Return a copy of this object which has been marked as deleted,
    * with the given notes appended.
    */
  def markDeleted(deletionNote: String): M

  /**
    * Return a copy of this object with given document status.
    */
  def withDocumentStatus(documentStatus: Option[DocumentStatus]): M

  /**
    * Return a copy of this with files transformed by applying `pathMapper`.
    *
    * @param pathMapper of files from source to destination URI
    * @return new metadata
    */
  protected def mapMove(pathMapper: (Option[URI], String) => Option[URI]): M

  /**
    * Return a copy of this object's notes with the given note appended.
    */
  protected def appendNote(note: String): Some[String] =
    notes match {
      case Some(existing) => Some(s"$existing\n$note")
      case None           => Some(note)
    }
}

object Metadata {

  val documentStatusFieldName = "document_stats"

  def jsonWithDocumentStatus(input: Json, documentStatus: DocumentStatus): Json = {
    import org.broadinstitute.clio.util.json.ModelAutoDerivation.encodeEnum
    input.mapObject(
      _.add(
        "document_status",
        documentStatus.asJson
      )
    )
  }

  /**
    * Given a `source` path, a `destination` directory, and an extension, figure out what
    * the new file name should be were the source file to be moved into the destination path,
    * optionally changing the base-name in the process.
    *
    * NOTE: Because we use '.' liberally in both our file names and file extensions,
    * there's no easy formula at this abstract level for determining which part of a
    * filename should be replaced by "newBasename" and which should be retained as the
    * extension. Rather than make dangerous guesses, we require that the code calling
    * the move operation provide the file extension to retain during the move operation.
    */
  def buildFilePath(
    source: URI,
    destination: URI,
    extension: String,
    newBasename: Option[String] = None
  ): URI = {
    val srcName = new File(source.getPath).getName
    val srcBase = srcName.take(srcName.lastIndexOf(extension))
    destination.resolve(s"${newBasename.getOrElse(srcBase)}$extension")
  }

  /**
    * Given one of our `Metadata` classes, extract out all fields that
    * store path-related information into a map from 'fieldName' -> 'path'.
    *
    * Used to build a generic before-and-after move comparison to determine
    * which paths in a metadata object will actually be affected by the move.
    */
  def extractPaths[M <: Metadata[M]: ClassTag](metadata: M): Map[String, URI] = {
    val metadataMapper = new CaseClassMapper[M]
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
