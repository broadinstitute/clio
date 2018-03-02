package org.broadinstitute.clio.server.dataaccess.elasticsearch

import io.circe.{Encoder, Json}
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.broadinstitute.clio.util.model.{EntityId, UpsertId}

import scala.reflect.ClassTag

/**
  * Maps metadata to an Elasticsearch document.
  *
  * @tparam Key      The type of the TransferKey.
  * @tparam Metadata The type of the TransferMetadata.
  */
class ElasticsearchDocumentMapper[Key <: Product: ClassTag, Metadata: ClassTag] (genId: () => UpsertId)(
  implicit
  private[elasticsearch] val keyEncoder: Encoder[Key],
  private[elasticsearch] val metadataEncoder: Encoder[Metadata]
) {
  private val keyMapper = new CaseClassMapper[Key]
  private val metadataMapper = new CaseClassMapper[Metadata]

  /**
    *
    * @param key      The TransferKey from which to create the document.
    * @param metadata The TransferMetadata from which to create the document.
    * @return
    */
  def document(key: Key, metadata: Metadata): Json = {
    val keyVals = keyMapper.vals(key)
    val metadataVals = metadataMapper.vals(metadata)
    val bookkeeping = Map(
      UpsertId.UpsertIdFieldName -> genId(),
      EntityId.EntityIdFieldName -> Symbol(
        key.productIterator.mkString(".")
      )
    )

    val keyJson = keyEncoder.apply(keyMapper.newInstance(keyVals))
    val metadataJson = metadataEncoder(metadataMapper.newInstance(metadataVals ++ bookkeeping))
    keyJson.deepMerge(metadataJson)
  }
}

object ElasticsearchDocumentMapper {

  def apply[Key <: Product: ClassTag, Metadata: ClassTag]: ElasticsearchDocumentMapper[Key, Metadata] = {
    new ElasticsearchDocumentMapper(UpsertId.nextId)
  }
}