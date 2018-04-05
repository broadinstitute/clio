package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ClioIndex

/**
  * Service responsible for running queries against a search DAO.
  */
class SearchService private[server] (searchDAO: SearchDAO) {

  /**
    * Run a raw Json query.
    *
    * @param json   The json string of the query.
    * @tparam CI    The index to query.
    * @return       The result of the query.
    */
  def rawQuery[CI <: ClioIndex](json: String)(
    implicit index: ElasticsearchIndex[CI]
  ): Source[Json, NotUsed] = {
    searchDAO
      .rawQuery(json)
  }
}
