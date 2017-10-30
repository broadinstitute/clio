package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.util.model.UpsertId

/**
  * Common trait for all document types Clio will store, used to force the inclusion of
  * bookkeeping fields in their type definitions.
  */
trait ClioDocument {

  /**
    * Uniquely identifies a partial document generated during an upsert operation.
    * Used both for debugging and for recording an ordering to metadata updates in
    * Clio's source of truth, so disaster recovery can determine the order in which
    * updates should be re-applied.
    */
  val upsertId: UpsertId

  /**
    * The unique key for an entity referred to by a document, used for tracking updates
    * to the metadata for an entity over time.
    *
    * We include this in our document JSON so that each JSON is self-contained, and
    * can be read from the source of truth and directly upserted into Elasticsearch
    * during recovery without needing to use a document mapper.
    */
  val entityId: Symbol

  /**
    * The filename used for persisting a document's upsert data.
    *
    * @return the filename where this document's upsert data is stored
    */
  def persistenceFilename: String = s"${upsertId.id}.json"
}

object ClioDocument {

  /**
    * Name of the ID field used in all Clio documents to identify a specific upsert.
    *
    * Used by the auto-document-mapper to generate IDs on the fly, and by
    * the auto-query-mapper to strip the IDs from returned query outputs.
    */
  val UpsertIdFieldName: String = "upsertId"

  /**
    * Elasticsearch name for the upsert ID field in a document.
    *
    * This name is used by elasticsearch to uniquely identify each document (upsert)
    * and to provide an ordering over the documents.
    */
  val UpsertIdElasticSearchName: String =
    ElasticsearchUtil.toElasticsearchName(UpsertIdFieldName)

  /**
    * Name of the ID field used in all Clio documents to identify a specific entity.
    *
    * Used by the auto-document-mapper to inject IDs on the fly, and by the
    * auto-query-mapper to strips the IDs from returned query outputs.
    */
  val EntityIdFieldName: String = "entityId"
}
