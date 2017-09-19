package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import org.broadinstitute.clio.util.generic.{
  CaseClassMapper,
  CaseClassMapperWithTypes,
  FieldMapper
}

import scala.reflect.ClassTag

/**
  * Builds an ElasticsearchQueryMapper using shapeless and reflection.
  *
  * @tparam ModelQueryInput  The type of the query input, with a context bound also specifying that both an
  *                          `implicit ctagQueryInput: ClassTag[ModelQueryInput]` exists, plus an
  *                          `implicit fieldMapper: FieldMapper[ModelQueryInput]` exists.
  *                          https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @tparam ModelQueryOutput The type of the query output, with a context bound also specifying that an
  *                          `implicit ctagQueryOutput: ClassTag[ModelQueryOutput]` exists.
  *                          https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @tparam Document         The Elasticsearch documents being queried, with a context bound also specifying that an
  *                          `implicit ctagDocument: ClassTag[Document]` exists.
  */
class AutoElasticsearchQueryMapper[
  ModelQueryInput: ClassTag: FieldMapper,
  ModelQueryOutput: ClassTag,
  Document <: ClioDocument: ClassTag
] private[dataaccess]
    extends ElasticsearchQueryMapper[
      ModelQueryInput,
      ModelQueryOutput,
      Document
    ] {
  val inputMapper = new CaseClassMapperWithTypes[ModelQueryInput]
  val outputMapper = new CaseClassMapper[ModelQueryOutput]
  val documentMapper = new CaseClassMapper[Document]

  override def isEmpty(queryInput: ModelQueryInput): Boolean = {
    flattenVals(queryInput).isEmpty
  }

  override def buildQuery(queryInput: ModelQueryInput): QueryDefinition = {
    val queries = flattenVals(queryInput) map {
      case (name, value) => build(name, value)
    }
    boolQuery must queries
  }

  override def toQueryOutput(document: Document): ModelQueryOutput = {
    val vals = documentMapper.vals(document)
    outputMapper.newInstance(
      vals -- Seq(
        ClioDocument.UpsertIdFieldName,
        ClioDocument.EntityIdFieldName
      )
    )
  }

  /**
    * Builds a query definition from a field name and a field value.
    *
    * @param name  The field name.
    * @param value The field value, already unwrapped from an option.
    * @return The query definition.
    */
  private def build(name: String, value: Any): QueryDefinition = {
    import ElasticsearchQueryMapper._

    import scala.reflect.runtime.universe.typeOf

    val esName = ElasticsearchUtil.toElasticsearchName(name)

    // Get the scala Type of the field and match the types. All filter fields are wrapped in an Option.
    inputMapper.types(name) match {
      case tpe
          if tpe =:= typeOf[Option[OffsetDateTime]] && esName.endsWith(
            "_start"
          ) =>
        queryOnOrAfter(
          esName.stripSuffix("_start"),
          value.asInstanceOf[OffsetDateTime]
        )
      case tpe
          if tpe =:= typeOf[Option[OffsetDateTime]] && esName.endsWith(
            "_end"
          ) =>
        queryOnOrBefore(
          esName.stripSuffix("_end"),
          value.asInstanceOf[OffsetDateTime]
        )
      case _ => queryVal(esName, value)
    }
  }

  /**
    * Returns the unwrapped values of a query input that are not None, sorted by name.
    *
    * @param queryInput The query input.
    * @return The sequence of field names and field values where the values weren't None.
    */
  private def flattenVals(queryInput: ModelQueryInput): Seq[(String, _)] = {
    val vals = inputMapper.vals(queryInput).toSeq
    val flattened = vals flatMap {
      case (name, valueOption) =>
        val tpe = inputMapper.types(name)
        if (!valueOption.isInstanceOf[Option[_]]) {
          throw new IllegalArgumentException(
            s"Unexpected non-Option value '$valueOption' for $name: $tpe"
          )
        }
        valueOption.asInstanceOf[Option[_]].map(name -> _)
    }
    flattened sortBy {
      case (name, _) => name
    }
  }
}

object AutoElasticsearchQueryMapper {
  def apply[QueryInput: ClassTag: FieldMapper,
            QueryOutput: ClassTag,
            Document <: ClioDocument: ClassTag]
    : ElasticsearchQueryMapper[QueryInput, QueryOutput, Document] = {
    new AutoElasticsearchQueryMapper
  }
}
