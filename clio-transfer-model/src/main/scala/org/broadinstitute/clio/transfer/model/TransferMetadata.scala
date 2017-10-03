package org.broadinstitute.clio.transfer.model

/**
  * Common information for metadata in all clio indexes.
  */
trait TransferMetadata[M <: TransferMetadata[M]] { self: M =>

  /**
    * Notes used to track changes to metadata (primarily deletions).
    */
  val notes: Option[String]

  /**
    * Paths to all files that should be moved by the clio-client move
    * command for this metadata's index.
    */
  def pathsToMove: Seq[String]

  /**
    * Return a copy of this object in which all files in `pathsToMove`
    * have been moved into a destination directory.
    *
    * Fails if `destination` doesn't end with '/'.
    */
  def moveAllInto(destination: String): M

  /**
    * Returns copy of this object in which a singular file-path has
    * been sent to a new destination.
    *
    * Fails if `pathsToMove` contains more than one element.
    */
  def setSinglePath(destination: String): M

  /**
    * Rebase the path `source` onto the directory path `destination`.
    */
  protected def moveInto(source: String, destination: String): String = {
    // TODO: Rewrite this using a Path-based API.
    destination + source.splitAt(source.lastIndexOf('/'))
  }
}
