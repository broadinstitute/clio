package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}
import com.sksamuel.elastic4s.HitReader

import scala.concurrent.{ExecutionContext, Future}

/**
  * Service responsible for running queries against a search DAO.
  */
class SearchService private (searchDAO: SearchDAO) {

  /**
    * Run a query.
    *
    * @param transferInput The DTO for the query input.
    * @param index         The index to search.
    * @param queryMapper   Converts the DTO into a search query.
    * @tparam TI The type of the Transfer Query Input DTO.
    * @tparam TO The type of the Transfer Query Output DTO.
    * @tparam D  The type of the Document.
    * @return The result of the query.
    */
  def queryMetadata[TI, TO, D: HitReader](
    transferInput: TI,
    index: ElasticsearchIndex[D],
    queryMapper: ElasticsearchQueryMapper[TI, TO, D]
  )(implicit ec: ExecutionContext): Future[Seq[TO]] = {
    if (queryMapper.isEmpty(transferInput)) {
      Future.successful(Seq.empty)
    } else {
      searchDAO
        .queryMetadata(queryMapper.buildQuery(transferInput), index)
        .map(queryMapper.toQueryOutputs)
    }
  }
}

object SearchService {
  def apply(app: ClioApp): SearchService = {
    new SearchService(app.searchDAO)
  }
}
