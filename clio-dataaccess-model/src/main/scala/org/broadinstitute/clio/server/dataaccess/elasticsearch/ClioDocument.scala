package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.util.UUID

/**
  * Common trait for all document types Clio will store, used to force the inclusion of a UUID
  * in their type definitions.
  */
trait ClioDocument {

  /**
    * Uniquely identifies a document instance. Used both for debugging and for recording
    * an ordering to metadata updates in Clio's source of truth, so disaster recovery can
    * determine the order in which updates should be re-applied.
    */
  val clioId: UUID
}

object ClioDocument {

  /**
    * Name of the ID field used in all Clio documents.
    *
    * Used by the auto-document-mapper to generate UUIDs on the fly, and by
    * the auto-query-mapper to strip the IDs from returned query outputs.
    */
  val IdFieldName: String = "clioId"
}
