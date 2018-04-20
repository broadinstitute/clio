package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ArraysIndex

import scala.concurrent.ExecutionContext

/**
  * Service responsible for performing all arrays-specific logic
  * before handing off to the generic search / persistence services.
  */
class ArraysService(
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO
)(implicit executionContext: ExecutionContext)
    extends IndexService(
      persistenceDAO,
      searchDAO,
      ElasticsearchIndex.Arrays,
      ArraysIndex
    )
