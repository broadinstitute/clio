package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries._
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation

/**
  * Service responsible for running queries against a search DAO.
  */
class SearchService private[server] (searchDAO: SearchDAO) {

  /**
    * Run a simple string query.
    * @param simpleJson The simple json to query.
    * @tparam CI The type of the TransferKey to query.
    * @return The result of the query.
    */
  def simpleStringQueryMetadata[Input, CI <: ClioIndex](simpleJson: Json)(
    implicit index: ElasticsearchIndex[CI],
    queryMapper: ElasticsearchQueryMapper[Input]
  ): Source[Json, NotUsed] = {
    searchDAO
      .queryMetadata(
        SimpleStringQueryDefinition(simpleJson.pretty(ModelAutoDerivation.defaultPrinter))
      )
      .map(queryMapper.toQueryOutput)
  }

  /**
    * Run a query.
    *
    * @param input The DTO for the query input.
    * @param queryMapper   Converts the DTO into a search query.
    * @tparam Input The type of the Transfer Query Input DTO.
    * @tparam CI The type of the TransferKey to query.
    * @return The result of the query.
    */
  def queryMetadata[Input, CI <: ClioIndex](
    input: Input,
    queryMapper: ElasticsearchQueryMapper[Input]
  )(
    implicit index: ElasticsearchIndex[CI]
  ): Source[Json, NotUsed] = {
    if (queryMapper.isEmpty(input)) {
      Source.empty[Json]
    } else {
      searchDAO
        .queryMetadata(queryMapper.buildQuery(input))
        .map(queryMapper.toQueryOutput)
    }
  }
}
