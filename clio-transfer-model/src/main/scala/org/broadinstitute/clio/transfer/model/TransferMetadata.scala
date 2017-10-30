package org.broadinstitute.clio.transfer.model

import java.net.URI

import org.broadinstitute.clio.util.model.DocumentStatus

/**
  * Common information for metadata in all clio indexes.
  */
trait TransferMetadata[M <: TransferMetadata[M]] { self: M =>

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
    * Paths to all files that should be moved by the clio-client move
    * command for this metadata's index.
    */
  def pathsToMove: Seq[URI]

  /**
    * Paths to all files that should be deleted by the clio-client delete
    * command for this metadata's index.
    */
  def pathsToDelete: Seq[URI]

  /**
    * Return a copy of this object in which all files in `pathsToMove`
    * have been moved to the given destination.
    *
    * If `pathsToMove` contains > 1 element, `destination` must end with '/'.
    */
  def moveInto(destination: URI): M = {
    val pathMapper =
      if (destination.getPath.endsWith("/")) { (opt: Option[URI]) =>
        opt.map(path => moveIntoDirectory(path, destination))
      } else if (pathsToMove.length > 1) {
        sys.error(
          "Non-directory destination given for metadata with > 1 path to move"
        )
      } else { (opt: Option[URI]) =>
        opt.map(_ => destination)
      }
    mapMove(pathMapper)
  }

  /**
    * Return a copy of this object which has been marked as deleted,
    * with the given notes appended.
    */
  def markDeleted(deletionNote: String): M

  /**
    * Return a copy of this object in which all files in `pathsToMove`
    * have been transformed by applying `pathMapper`.
    */
  protected def mapMove(pathMapper: Option[URI] => Option[URI]): M

  /**
    * Move the file at `source` into the directory `destination`.
    */
  protected def moveIntoDirectory(source: URI, destination: URI): URI = {
    // TODO: Rewrite this using a Path-based API.
    val srcPath = source.getPath
    destination.resolve(srcPath.substring(srcPath.lastIndexOf('/') + 1))
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
