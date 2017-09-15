package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.util.UUID

/**
  * Common trait for all document types Clio will store, used to force the inclusion of
  * bookkeeping fields in their type definitions.
  */
trait ClioDocument {

  /**
    * Uniquely identifies a document instance. Used both for debugging and for recording
    * an ordering to metadata updates in Clio's source of truth, so disaster recovery can
    * determine the order in which updates should be re-applied.
    */
  val clioId: UUID

  /**
    * The unique key for an entity referred to by a document, used for tracking updates
    * to the metadata for an entity over time.
    *
    * We include this in our document JSON so that each JSON is self-contained, and
    * can be read from the source of truth and directly upserted into Elasticsearch
    * during recovery without needing to use a document mapper.
    */
  val entityId: String
}

object ClioDocument {

  /**
    * Name of the ID field used in all Clio documents to identify a specific upsert.
    *
    * Used by the auto-document-mapper to generate UUIDs on the fly, and by
    * the auto-query-mapper to strip the IDs from returned query outputs.
    */
  val UpsertIdFieldName: String = "clioId"

  /**
    * Name of the ID field used in all Clio documents to identify a specific entity.
    *
    * Used by the auto-document-mapper to inject IDs on the fly, and by the
    * auto-query-mapper to strips the IDs from returned query outputs.
    */
  val EntityIdFieldName: String = "entityId"
}
