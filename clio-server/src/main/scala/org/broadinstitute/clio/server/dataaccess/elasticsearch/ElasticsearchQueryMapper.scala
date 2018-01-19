package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.searches.queries.QueryDefinition

/**
  * Maps query input and outputs.
  *
  * @tparam ModelQueryInput  The type of the query input.
  * @tparam ModelQueryOutput The type of the query output.
  * @tparam Document         The Elasticsearch documents being queried.
  */
abstract class ElasticsearchQueryMapper[ModelQueryInput, ModelQueryOutput, Document] {

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
  def buildQuery(queryInput: ModelQueryInput)(
    implicit index: ElasticsearchIndex[Document]
  ): QueryDefinition

  /**
    * Converts the query result document to a query output.
    *
    * @param document A query result document.
    * @return The query output.
    */
  def toQueryOutput(document: Document): ModelQueryOutput
}
