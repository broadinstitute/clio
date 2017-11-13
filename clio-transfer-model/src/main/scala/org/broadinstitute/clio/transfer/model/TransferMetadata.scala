package org.broadinstitute.clio.transfer.model

import java.io.File
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
    * Paths to all files that should be deleted by the clio-client delete
    * command for this metadata's index.
    */
  def pathsToDelete: Seq[URI]

  /**
    * Return a copy of this object in which all files have been moved
    * to the given `destination` directory, optionally prefixed with
    * `samplePrefix`.
    *
    * Until we move to a Path-based API, `destination` must end with '/'.
    */
  def moveInto(destination: URI, samplePrefix: Option[String] = None): M = {
    if (!destination.getPath.endsWith("/")) {
      sys.error(
        s"Non-directory destination '$destination' given for metadata move"
      )
    }

    mapMove(
      samplePrefix,
      opt => opt.map(path => moveIntoDirectory(path, destination))
    )
  }

  /**
    * Return a copy of this object which has been marked as deleted,
    * with the given notes appended.
    */
  def markDeleted(deletionNote: String): M

  /**
    * Return a copy of this with files transformed by applying
    * `pathMapper` and `samplePrefix`.
    *
    * @param samplePrefix added to files derived from sample names
    * @param pathMapper of files from source to destination URI
    * @return new metadata
    */
  protected def mapMove(samplePrefix: Option[String] = None,
                        pathMapper: Option[URI] => Option[URI]): M

  /**
    * Move the file at `source` into the directory `destination`.
    */
  protected def moveIntoDirectory(source: URI, destination: URI): URI = {
    // TODO: Rewrite this using a Path-based API.
    val srcName = new File(source.getPath).getName
    destination.resolve(srcName)
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
