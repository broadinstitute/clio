package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.util.generic.FieldMapper

import com.sksamuel.elastic4s.mappings.FieldDefinition

import scala.reflect.{ClassTag, classTag}

/**
  * An index for an Elasticsearch document.
  *
  * @tparam Document The type of the Elasticsearch document.
  */
abstract class ElasticsearchIndex[Document] {

  /** The name of the index. */
  def indexName: String

  /**
    * The name of the index type. Always default until ES 7 when there will be no index types.
    * https://www.elastic.co/blog/elasticsearch-6-0-0-alpha1-released#type-removal
    */
  final val indexType: String = "default"

  /** The fields for the index. */
  def fields: Seq[FieldDefinition]
}

object ElasticsearchIndex {
  val WgsUbam: ElasticsearchIndex[DocumentWgsUbam] =
    indexDocument[DocumentWgsUbam]

  /**
    * Creates an index using shapeless and reflection.
    *
    * @tparam Document The type of the Elasticsearch document, with a context bound also specifying that both an
    *                  `implicit ctag: ClassTag[Document]` exists, plus an
    *                  `implicit fieldMapper: FieldMapper[Document]` exists.
    *                  https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @return The index.
    */
  private[dataaccess] def indexDocument[Document: ClassTag: FieldMapper]
    : ElasticsearchIndex[Document] = {
    import s_mach.string._
    val nameSnake = classTag[Document].runtimeClass.getSimpleName
      .toSnakeCase(Lexer.CamelCase)
      .stripPrefix("document_")
    new AutoElasticsearchIndex[Document](nameSnake)
  }
}