package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}
import org.broadinstitute.clio.transfer.model.TransferIndex

/**
  * Service responsible for running queries against a search DAO.
  */
class SearchService private[server] (searchDAO: SearchDAO) {

  /**
    * Run a query.
    *
    * @param transferInput The DTO for the query input.
    * @param queryMapper   Converts the DTO into a search query.
    * @tparam Input The type of the Transfer Query Input DTO.
    * @tparam TI The type of the TransferKey to query.
    * @return The result of the query.
    */
  def queryMetadata[Input, TI <: TransferIndex](
    transferInput: Input,
    queryMapper: ElasticsearchQueryMapper[Input]
  )(
    implicit index: ElasticsearchIndex[TI]
  ): Source[Json, NotUsed] = {
    if (queryMapper.isEmpty(transferInput)) {
      Source.empty[Json]
    } else {
      searchDAO
        .queryMetadata(queryMapper.buildQuery(transferInput))
        .map(queryMapper.toQueryOutput)
    }
  }
}
