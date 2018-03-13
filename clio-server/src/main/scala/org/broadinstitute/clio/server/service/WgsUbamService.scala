package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsUbamIndex

import scala.concurrent.ExecutionContext

/**
  * Service responsible for performing all wgs-ubam-specific logic
  * before handing off to the generic search / persistence services.
  */
class WgsUbamService(
  persistenceService: PersistenceService,
  searchService: SearchService
)(implicit executionContext: ExecutionContext)
    extends IndexService[WgsUbamIndex.type](
      persistenceService,
      searchService,
      ElasticsearchIndex.WgsUbam,
      WgsUbamIndex
    )
