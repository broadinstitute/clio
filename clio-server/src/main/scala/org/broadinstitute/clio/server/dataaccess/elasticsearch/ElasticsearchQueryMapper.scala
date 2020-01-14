package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl.boolQuery
import com.sksamuel.elastic4s.http.search.queries.compound.BoolQueryBuilderFn
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Json, JsonObject}
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

  val elasticsearchQueryObjectName = "query"
  private val keysToDrop =
    Set(
      ElasticsearchIndex.UpsertIdElasticsearchName,
      ElasticsearchIndex.EntityIdElasticsearchName
    )

  /**
    * Builds an elastic4s query as a json string from the query input.
    *
    * @param queryInput The query input.
    * @return A json object representing an Elasticsearch query
    */
  def buildQuery(queryInput: Input)(
    implicit index: ElasticsearchIndex[_]
  ): JsonObject = {
    val queries = flattenVals(queryInput) map {
      case (name, value) =>
        index.fieldMapper.valueToQuery(
          ElasticsearchUtil.toElasticsearchName(name),
          inputMapper.typeOf(name)
        )(value)
    }
    val queryDefinition = boolQuery must queries
    JsonObject(
      (
        elasticsearchQueryObjectName,
        parse(BoolQueryBuilderFn(queryDefinition).string).fold(throw _, identity)
      )
    )
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
      case (name, value) =>
        value match {
          case opt: Option[_] => opt.map(name -> _)
          case nonOpt         => Some(name -> nonOpt)
        }
    }
    flattened sortBy {
      case (name, _) => name
    }
  }

  /**
    * Munges the query result document into the appropriate form for the query output.
    *
    * @param document A query result document.
    * @return The query output.
    */
  def toQueryOutput(document: Json): Json = {
    val result = document.mapObject(_.filterKeys(!keysToDrop.contains(_)))
      .fold[Json](().asJson, _.asJson, _.asJson, _.asJson, _.asJson, _.asJson)
    result
  }
}

object ElasticsearchQueryMapper {

  def apply[Input: ClassTag: FieldMapper]: ElasticsearchQueryMapper[Input] = {
    new ElasticsearchQueryMapper[Input]
  }
}
