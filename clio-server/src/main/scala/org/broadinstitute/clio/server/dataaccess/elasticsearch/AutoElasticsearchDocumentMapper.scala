package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.broadinstitute.clio.util.model.{UpsertId, EntityId}

import scala.reflect.ClassTag

/**
  * Builds an ElasticsearchDocumentMapper using shapeless and reflection.
  *
  * @tparam ModelKey      The type of key, extending from scala.Product, with a context bound also specifying that an
  *                       `implicit ctagKey: ClassTag[ModelKey]` exists.
  *                       https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @tparam ModelMetadata The type of the metadata, with a context bound also specifying that an
  *                       `implicit ctagMetadata: ClassTag[ModelMetadata]` exists.
  *                       https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  *                       https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  */
class AutoElasticsearchDocumentMapper[
  ModelKey <: Product: ClassTag,
  ModelMetadata: ClassTag,
] private[elasticsearch] (genId: () => UpsertId)
    extends ElasticsearchDocumentMapper[ModelKey, ModelMetadata] {

  private val modelKeyMapper = new CaseClassMapper[ModelKey]
  private val modelMetadataMapper = new CaseClassMapper[ModelMetadata]

  override def empty(key: ModelKey): (ModelKey, ModelMetadata) = {
    val keyVals = modelKeyMapper.vals(key)
    val bookkeeping = Map(
      UpsertId.UpsertIdFieldName -> genId(),
      EntityId.EntityIdFieldName -> Symbol(
        key.productIterator.mkString(".")
      )
    )
    val modelKey = modelKeyMapper.newInstance(keyVals ++ bookkeeping)
    val modelMetadata = modelMetadataMapper.newInstance(Map.empty)
    (modelKey, modelMetadata)
  }

  override def withMetadata(key: ModelKey, existingMetadata: ModelMetadata, newMetadata: ModelMetadata): (ModelKey, ModelMetadata) = {
    // Copy all the metadata values except those that are `None`.
    val metadataVals = modelMetadataMapper vals newMetadata collect {
      case (name, value) if value != None => (name, value)
    }
    val copy = modelMetadataMapper.copy(existingMetadata, metadataVals)
    (key, copy)
  }
}

object AutoElasticsearchDocumentMapper {

  def apply[
    ModelKey <: Product: ClassTag,
    ModelMetadata: ClassTag,
  ]: ElasticsearchDocumentMapper[ModelKey, ModelMetadata] = {
    new AutoElasticsearchDocumentMapper(UpsertId.nextId)
  }
}
