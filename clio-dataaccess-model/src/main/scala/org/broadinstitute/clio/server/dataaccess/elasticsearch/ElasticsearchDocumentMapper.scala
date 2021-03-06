package org.broadinstitute.clio.server.dataaccess.elasticsearch

import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.broadinstitute.clio.transfer.model.IndexKey
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

/**
  * Maps metadata to an Elasticsearch document.
  *
  * @tparam Key      The type of the IndexKey.
  * @tparam Metadata The type of the Metadata.
  */
class ElasticsearchDocumentMapper[Key <: IndexKey: Encoder, Metadata: Encoder](
  genId: () => UpsertId
) extends ModelAutoDerivation {

  /**
    *
    * @param key      The IndexKey from which to create the document.
    * @param metadata The Metadata from which to create the document.
    * @return
    */
  def document(key: Key, metadata: Metadata): Json = {
    val bookkeeping = Json.obj(
      ElasticsearchIndex.UpsertIdElasticsearchName -> genId().asJson
    )

    val keyJson = key.asJson
    val metadataJson = metadata.asJson
    keyJson
      .deepMerge(metadataJson)
      .deepMerge(bookkeeping)
  }
}

object ElasticsearchDocumentMapper {

  def apply[
    Key <: IndexKey: Encoder,
    Metadata: Encoder
  ]: ElasticsearchDocumentMapper[Key, Metadata] = {
    new ElasticsearchDocumentMapper[Key, Metadata](UpsertId.nextId)
  }
}
