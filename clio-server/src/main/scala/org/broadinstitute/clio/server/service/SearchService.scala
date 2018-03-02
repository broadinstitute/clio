package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{ElasticsearchIndex, ElasticsearchQueryMapper}
import org.broadinstitute.clio.transfer.model.TransferIndex

/**
  * Service responsible for running queries against a search DAO.
  */
class SearchService private (searchDAO: SearchDAO) {

  /**
    * Run a query.
    *
    * @param transferInput The DTO for the query input.
    * @param queryMapper   Converts the DTO into a search query.
    * @tparam Input The type of the Transfer Query Input DTO.
    * @tparam Index The type of the TransferKey to query.
    * @return The result of the query.
    */
  def queryMetadata[Input, Index <: TransferIndex](
    transferInput: Input,
    queryMapper: ElasticsearchQueryMapper[Input]
  )(
    implicit index: ElasticsearchIndex[Index]
  ): Source[Json, NotUsed] = {
    if (queryMapper.isEmpty(transferInput)) {
      Source.empty[Json]
    } else {
      searchDAO.queryMetadata(queryMapper.buildQuery(transferInput))
    }
  }
}

object SearchService {

  def apply(app: ClioApp): SearchService = {
    new SearchService(app.searchDAO)
  }
}
