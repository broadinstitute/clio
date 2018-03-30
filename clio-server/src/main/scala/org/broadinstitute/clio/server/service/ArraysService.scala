package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ArraysIndex

import scala.concurrent.ExecutionContext

/**
  * Service responsible for performing all arrays-specific logic
  * before handing off to the generic search / persistence services.
  */
class ArraysService(
  persistenceService: PersistenceService,
  searchService: SearchService
)(implicit executionContext: ExecutionContext)
    extends IndexService(
      persistenceService,
      searchService,
      ElasticsearchIndex.Arrays,
      ArraysIndex
    )