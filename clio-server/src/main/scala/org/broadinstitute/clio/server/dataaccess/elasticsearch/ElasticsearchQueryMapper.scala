package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import com.sksamuel.elastic4s.HitReader
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import org.broadinstitute.clio.server.model.{
  ModelWgsUbamQueryInput,
  ModelWgsUbamQueryOutput
}

/**
  * Maps query input and outputs.
  *
  * @tparam ModelQueryInput  The type of the query input.
  * @tparam ModelQueryOutput The type of the query output.
  * @tparam Document         The Elasticsearch documents being queried.
  */
abstract class ElasticsearchQueryMapper[ModelQueryInput,
                                        ModelQueryOutput,
                                        Document] {

  /**
    * Returns true if the client sent a query that doesn't contain any filters.
    *
    * @param queryInput The query input.
    * @return True if the client sent a query that doesn't contain any filters.
    */
  def isEmpty(queryInput: ModelQueryInput): Boolean

  /**
    * Builds an elastic4s query definition from the query input.
    *
    * @param queryInput The query input.
    * @return An elastic4s query definition from the query input.
    */
  def buildQuery(queryInput: ModelQueryInput): QueryDefinition

  /**
    * Converts the query result document to a query output.
    *
    * @param document A query result document.
    * @return The query output.
    */
  def toQueryOutput(document: Document): ModelQueryOutput

  private[dataaccess] def toQueryOutputs(
    searchResponse: SearchResponse
  )(implicit hitReader: HitReader[Document]): Seq[ModelQueryOutput] = {
    searchResponse.to[Document].map(toQueryOutput)
  }
}

object ElasticsearchQueryMapper {
  private[dataaccess] val WgsUbam =
    AutoElasticsearchQueryMapper[
      ModelWgsUbamQueryInput,
      ModelWgsUbamQueryOutput,
      DocumentWgsUbam
    ]

  /**
    * Build an elastic4s query definition for a start date.
    *
    * @param name  The field name.
    * @param value The minimum start date.
    * @return The query definition.
    */
  private[dataaccess] def queryOnOrAfter(
    name: String,
    value: OffsetDateTime
  ): QueryDefinition = {
    rangeQuery(name).gte(value.toString)
  }

  /**
    * Build an elastic4s query definition for an end date.
    *
    * @param name  The field name.
    * @param value The maximum end date.
    * @return The query definition.
    */
  private[dataaccess] def queryOnOrBefore(
    name: String,
    value: OffsetDateTime
  ): QueryDefinition = {
    rangeQuery(name).lte(value.toString)
  }

  /**
    * Query a field for an exact value.
    *
    * @param name  The field name.
    * @param value The exact field value.
    * @return The query definition.
    */
  private[dataaccess] def queryVal(name: String, value: Any): QueryDefinition = {
    queryStringQuery(s""""$value"""").defaultField(name)
  }
}
