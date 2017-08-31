package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.server.dataaccess.util.ClioUUIDGenerator
import org.broadinstitute.clio.util.generic.CaseClassMapper

import scala.reflect.ClassTag

import java.util.UUID

/**
  * Builds an ElasticsearchDocumentMapper using shapeless and reflection.
  *
  * @tparam ModelKey      The type of key, extending from scala.Product, with a context bound also specifying that an
  *                       `implicit ctagKey: ClassTag[ModelKey]` exists.
  *                       https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @tparam ModelMetadata The type of the metadata, with a context bound also specifying that an
  *                       `implicit ctagMetadata: ClassTag[ModelMetadata]` exists.
  *                       https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @tparam Document      The type of the document, with a context bound also specifying that an
  *                       `implicit ctagDocument: ClassTag[Document]` exists.
  *                       https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  */
class AutoElasticsearchDocumentMapper[
  ModelKey <: Product: ClassTag, ModelMetadata: ClassTag,
  Document <: ClioDocument: ClassTag
] private[elasticsearch] (genId: () => UUID)
    extends ElasticsearchDocumentMapper[ModelKey, ModelMetadata, Document] {

  private val modelKeyMapper = new CaseClassMapper[ModelKey]
  private val modelMetadataMapper = new CaseClassMapper[ModelMetadata]
  private val documentMapper = new CaseClassMapper[Document]

  override def id(key: ModelKey): String = key.productIterator.mkString(".")

  override def empty(key: ModelKey): Document = {
    val keyVals = modelKeyMapper.vals(key)
    val document = documentMapper.newInstance(
      keyVals + (ClioDocument.IdFieldName -> genId())
    )
    document
  }

  override def withMetadata(document: Document,
                            metadata: ModelMetadata): Document = {
    // Copy all the metadata values except those that are `None`.
    val metadataVals = modelMetadataMapper vals metadata collect {
      case (name, value) if value != None => (name, value)
    }
    val copy = documentMapper.copy(document, metadataVals)
    copy
  }
}

object AutoElasticsearchDocumentMapper {
  def apply[ModelKey <: Product: ClassTag,
            ModelMetadata: ClassTag,
            Document <: ClioDocument: ClassTag]
    : ElasticsearchDocumentMapper[ModelKey, ModelMetadata, Document] = {
    // Don't listen to IntelliJ's warning on the line below; removing
    // the _ results in a compiler deprecation warning, which our settings
    // escalate into an error.
    new AutoElasticsearchDocumentMapper(ClioUUIDGenerator.getUUID _)
  }
}
