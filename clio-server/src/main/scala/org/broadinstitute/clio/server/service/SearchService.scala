package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}
import org.broadinstitute.clio.transfer.model.ClioIndex

/**
  * Service responsible for running queries against a search DAO.
  */
class SearchService private[server] (searchDAO: SearchDAO) {

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
