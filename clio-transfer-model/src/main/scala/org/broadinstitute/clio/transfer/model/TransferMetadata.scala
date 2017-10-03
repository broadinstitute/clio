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
    * Rebase all paths in `pathsToMove` onto a destination directory path.
    *
    * Fails if `destination` doesn't end with '/'.
    */
  def rebaseOnto(destination: String): M

  /**
    * Set the singular path in this metadata to a new destination.
    *
    * Fails if `pathsToMove` contains more than one element.
    */
  def setSinglePath(destination: String): M

  /**
    * Rebase the path `source` onto the directory path `destination`.
    */
  protected def rebaseOnto(source: String, destination: String): String = {
    destination + source.splitAt(source.lastIndexOf('/'))
  }
}
