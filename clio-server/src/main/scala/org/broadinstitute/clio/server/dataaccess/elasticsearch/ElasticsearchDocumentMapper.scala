package org.broadinstitute.clio.server.dataaccess.elasticsearch

import io.circe.{Encoder, Json}
import io.circe.syntax._
import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

/**
  * Maps metadata to an Elasticsearch document.
  *
  * @tparam Key      The type of the TransferKey.
  * @tparam Metadata The type of the TransferMetadata.
  */
class ElasticsearchDocumentMapper[Key <: Product: Encoder, Metadata <: TransferMetadata[
  Metadata
]: Encoder](genId: () => UpsertId)
    extends ModelAutoDerivation {

  /**
    *
    * @param key      The TransferKey from which to create the document.
    * @param metadata The TransferMetadata from which to create the document.
    * @return
    */
  def document(key: Key, metadata: Metadata): Json = {
    val upsertIdMap = Map(
      ElasticsearchIndex.UpsertIdElasticsearchName -> genId()
    )
    val entityIdMap = Map(
      ElasticsearchIndex.EntityIdElasticsearchName -> Symbol(
        key.productIterator.mkString(".")
      )
    )

    val keyJson = key.asJson
    val metadataJson = metadata.asJson
    val upsertIdJson = upsertIdMap.asJson
    val entityIdJson = entityIdMap.asJson
    keyJson.deepMerge(metadataJson).deepMerge(upsertIdJson).deepMerge(entityIdJson)
  }
}

object ElasticsearchDocumentMapper {

  def apply[Key <: Product: Encoder, Metadata <: TransferMetadata[Metadata]: Encoder]: ElasticsearchDocumentMapper[
    Key,
    Metadata
  ] = {
    new ElasticsearchDocumentMapper[Key, Metadata](UpsertId.nextId)
  }
}
