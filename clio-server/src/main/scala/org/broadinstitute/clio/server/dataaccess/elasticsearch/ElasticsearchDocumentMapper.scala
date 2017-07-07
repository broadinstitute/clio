package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.server.model._

/**
  * Maps metadata to an Elasticsearch document.
  *
  * @tparam ModelKey      The type of key.
  * @tparam ModelMetadata The type of the metadata.
  * @tparam Document      The type of the document.
  */
abstract class ElasticsearchDocumentMapper[ModelKey, ModelMetadata, Document] {

  /**
    * Returns the document id for a key.
    *
    * @param key The key.
    * @return The document id.
    */
  def id(key: ModelKey): String

  /**
    * Returns an empty document for a key.
    *
    * @param key The key.
    * @return The newly initialized document.
    */
  def empty(key: ModelKey): Document

  /**
    * Returns an updated document with new metadata, where any fields that are `None` do NOT overwrite the existing
    * metadata.
    *
    * @param document The existing document.
    * @param metadata The new metadata.
    * @return The updated document.
    */
  def withMetadata(document: Document, metadata: ModelMetadata): Document
}

object ElasticsearchDocumentMapper {
  private[dataaccess] val ReadGroup = AutoElasticsearchDocumentMapper[
    ModelReadGroupKey,
    ModelReadGroupMetadata,
    DocumentReadGroup
  ]
}
