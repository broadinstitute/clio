package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsCramIndex

import scala.concurrent.ExecutionContext

/**
  * Service responsible for performing all wgs-cram-specific logic
  * before handing off to the generic search / persistence services.
  */
class WgsCramService(
  persistenceService: PersistenceService,
  searchService: SearchService
)(implicit executionContext: ExecutionContext)
    extends IndexService(
      persistenceService,
      searchService,
      ElasticsearchIndex.WgsCram,
      WgsCramIndex
    )
