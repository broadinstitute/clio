package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl.boolQuery
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import org.broadinstitute.clio.util.generic.{CaseClassMapperWithTypes, FieldMapper}

import scala.reflect.ClassTag

/**
  * Builds an ElasticsearchQueryMapper using shapeless and reflection.
  *
  * @tparam Input            The type of the query input, with a context bound also specifying that both an
  *                          `implicit ctagQueryInput: ClassTag[Input]` exists, plus an
  *                          `implicit fieldMapper: FieldMapper[Input]` exists.
  *                          https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  */
class ElasticsearchQueryMapper[Input: ClassTag: FieldMapper] {
  val inputMapper = new CaseClassMapperWithTypes[Input]

  /**
    * Returns true if the client sent a query that doesn't contain any filters.
    *
    * @param queryInput The query input.
    * @return True if the client sent a query that doesn't contain any filters.
    */
  def isEmpty(queryInput: Input): Boolean = {
    flattenVals(queryInput).isEmpty
  }

  /**
    * Builds an elastic4s query definition from the query input.
    *
    * @param queryInput The query input.
    * @return An elastic4s query definition from the query input.
    */
  def buildQuery(queryInput: Input)(
    implicit index: ElasticsearchIndex[_]
  ): QueryDefinition = {
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

  /**
    * Returns the unwrapped values of a query input that are not None, sorted by name.
    *
    * @param queryInput The query input.
    * @return The sequence of field names and field values where the values weren't None.
    */
  private def flattenVals(queryInput: Input): Seq[(String, _)] = {
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

object ElasticsearchQueryMapper {

  def apply[Input: ClassTag: FieldMapper]: ElasticsearchQueryMapper[Input] = {
    new ElasticsearchQueryMapper[Input]
  }
}

