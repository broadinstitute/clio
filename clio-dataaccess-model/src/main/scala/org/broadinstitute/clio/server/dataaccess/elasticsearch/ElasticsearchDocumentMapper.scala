package org.broadinstitute.clio.server.dataaccess.elasticsearch

import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

/**
  * Maps metadata to an Elasticsearch document.
  *
  * @tparam Key      The type of the TransferKey.
  * @tparam Metadata The type of the TransferMetadata.
  */
class ElasticsearchDocumentMapper[Key <: Product: Encoder, Metadata: Encoder](
  genId: () => UpsertId
) extends ModelAutoDerivation {

  /**
    *
    * @param key      The TransferKey from which to create the document.
    * @param metadata The TransferMetadata from which to create the document.
    * @return
    */
  def document(key: Key, metadata: Metadata): Json = {
    val bookkeeping = Json.fromFields(
      Seq(
        ElasticsearchIndex.UpsertIdElasticsearchName -> genId().asJson,
        ElasticsearchIndex.EntityIdElasticsearchName -> key.productIterator
          .mkString(".")
          .asJson
      )
    )

    val keyJson = key.asJson
    val metadataJson = metadata.asJson
    keyJson
      .deepMerge(metadataJson)
      .deepMerge(bookkeeping)
  }
}

object ElasticsearchDocumentMapper {

  def apply[Key <: Product: Encoder, Metadata: Encoder]: ElasticsearchDocumentMapper[
    Key,
    Metadata
  ] = {
    new ElasticsearchDocumentMapper[Key, Metadata](UpsertId.nextId)
  }
}
