package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import org.broadinstitute.clio.transfer.model.{TransferKey, TransferMetadata}
import org.broadinstitute.clio.util.generic.{CaseClassMapper, CaseClassMapperWithTypes, FieldMapper}
import org.broadinstitute.clio.util.model.{EntityId, UpsertId}

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
  * @tparam Key              The primary key of the Elasticsearch documents being queried, with a context bound also specifying that an
  *                          `implicit ctagKey: ClassTag[Key]` exists.
  * @tparam Metadata         The metadata content of the Elasticsearch documents being queried, with a context bound also
  *                          specifying that an `implicit ctagMetadata: ClassTag[Metadata]` exists.
  */
class AutoElasticsearchQueryMapper[
  ModelQueryInput: ClassTag: FieldMapper,
  ModelQueryOutput: ClassTag,
  Key <: TransferKey: ClassTag,
  Metadata <: TransferMetadata[Metadata]: ClassTag
] private[dataaccess]
    extends ElasticsearchQueryMapper[
      ModelQueryInput,
      ModelQueryOutput,
      Key,
      Metadata
    ] {
  val inputMapper = new CaseClassMapperWithTypes[ModelQueryInput]
  val outputMapper = new CaseClassMapper[ModelQueryOutput]
  val keyMapper = new CaseClassMapper[Key]
  val metadataMapper = new CaseClassMapper[Metadata]

  override def isEmpty(queryInput: ModelQueryInput): Boolean = {
    flattenVals(queryInput).isEmpty
  }

  override def buildQuery(
    queryInput: ModelQueryInput
  )(implicit index: ElasticsearchIndex[Key, Metadata]): QueryDefinition = {
    val queries = flattenVals(queryInput) map {
      case (name, value) => {
        index.fieldMapper.valueToQuery(
          ElasticsearchUtil.toElasticsearchName(name),
          inputMapper.types(name)
        )(value)
      }
    }
    boolQuery must queries
  }

  override def toQueryOutput(key: Key, metadata: Metadata): ModelQueryOutput = {
    val keyVals = keyMapper.vals(key)
    val metadataVals = metadataMapper.vals(metadata)
    outputMapper.newInstance(
      keyVals ++ metadataVals -- Seq(
        UpsertId.UpsertIdFieldName,
        EntityId.EntityIdFieldName
      )
    )
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

  def apply[
    QueryInput: ClassTag: FieldMapper,
    QueryOutput: ClassTag,
    Key <: TransferKey: ClassTag,
    Metadata <: TransferMetadata[Metadata]: ClassTag
  ]: ElasticsearchQueryMapper[QueryInput, QueryOutput, Key, Metadata] = {
    new AutoElasticsearchQueryMapper
  }
}
