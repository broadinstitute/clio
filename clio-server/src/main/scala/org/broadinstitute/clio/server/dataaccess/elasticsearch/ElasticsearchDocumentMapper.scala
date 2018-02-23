package org.broadinstitute.clio.server.dataaccess.elasticsearch

/**
  * Maps metadata to an Elasticsearch document.
  *
  * @tparam ModelKey      The type of key.
  * @tparam ModelMetadata The type of the metadata.
  */
abstract class ElasticsearchDocumentMapper[ModelKey, ModelMetadata] {

  /**
    * Returns an empty document for a key.
    *
    * @param key The key.
    * @return The newly initialized key and metadata.
    */
  def empty(key: ModelKey): (ModelKey, ModelMetadata)

  /**
    * Returns an updated document with new metadata, where any fields that are `None` do NOT overwrite the existing
    * metadata.
    *
    * @param key The existing key.
    * @param existingMetadata The existing metadata.
    * @param newMetadata The new metadata.
    * @return The updated document.
    */
  def withMetadata(key: ModelKey, existingMetadata: ModelMetadata, newMetadata: ModelMetadata): (ModelKey, ModelMetadata)
}
