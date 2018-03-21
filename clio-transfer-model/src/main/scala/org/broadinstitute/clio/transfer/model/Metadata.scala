package org.broadinstitute.clio.transfer.model

import java.io.File
import java.net.URI

import org.broadinstitute.clio.util.model.DocumentStatus

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
        pathOpt.map(moveIntoDirectory(_, destination, ext, newBasename))
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
    * Move the file at `source` into the directory `destination`,
    * optionally changing the base-name in the process.
    *
    * NOTE: Because we use '.' liberally in both our file names and file extensions,
    * there's no easy formula at this abstract level for determining which part of a
    * filename should be replaced by "newBasename" and which should be retained as the
    * extension. Rather than make dangerous guesses, we require that the code calling
    * the move operation provide the file extension to retain during the move operation.
    */
  protected def moveIntoDirectory(
    source: URI,
    destination: URI,
    extension: String,
    newBasename: Option[String] = None
  ): URI = {
    // TODO: Rewrite this using a Path-based API.
    val srcName = new File(source.getPath).getName
    val srcBase = srcName.take(srcName.toLowerCase.lastIndexOf(extension))
    destination.resolve(s"${newBasename.getOrElse(srcBase)}$extension")
  }

  /**
    * Return a copy of this object's notes with the given note appended.
    */
  protected def appendNote(note: String): Some[String] =
    notes match {
      case Some(existing) => Some(s"$existing\n$note")
      case None           => Some(note)
    }
}
