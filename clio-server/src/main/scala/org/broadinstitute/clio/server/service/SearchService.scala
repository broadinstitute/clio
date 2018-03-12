package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}

/**
  * Service responsible for running queries against a search DAO.
  */
class SearchService private (searchDAO: SearchDAO) {

  /**
    * Run a query.
    *
    * @param transferInput The DTO for the query input.
    * @param queryMapper   Converts the DTO into a search query.
    * @tparam TI The type of the Transfer Query Input DTO.
    * @tparam TO The type of the Transfer Query Output DTO.
    * @tparam D  The type of the Document.
    * @return The result of the query.
    */
  def queryMetadata[TI, TO, D <: ClioDocument: ElasticsearchIndex](
    transferInput: TI,
    queryMapper: ElasticsearchQueryMapper[TI, TO, D]
  ): Source[TO, NotUsed] = {
    if (queryMapper.isEmpty(transferInput)) {
      Source.empty[TO]
    } else {
      searchDAO
        .queryMetadata(queryMapper.buildQuery(transferInput))
        .map(queryMapper.toQueryOutput)
    }
  }
}

object SearchService {

  def apply(searchDAO: SearchDAO): SearchService = {
    new SearchService(searchDAO)
  }
}
