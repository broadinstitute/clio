package org.broadinstitute.clio.transfer.model

import java.net.URI

import io.circe.Json
import io.circe.syntax._
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
    * Return a copy of this object that has been marked with the given
    * DocumentStatus, with the given notes appended.
    */
  def changeStatus(documentStatus: DocumentStatus, changeNote: String): M

  /**
    * Return a copy of this object with given document status.
    */
  def withDocumentStatus(documentStatus: Option[DocumentStatus]): M

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
  
  def jsonWithDocumentStatus(input: Json, documentStatus: DocumentStatus): Json = {
    import org.broadinstitute.clio.util.json.ModelAutoDerivation.encodeEnum
    input.mapObject(
      _.add(
        "document_status",
        documentStatus.asJson
      )
    )
  }
}
