package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.GvcfIndex

import scala.concurrent.ExecutionContext

/**
  * Service responsible for performing all gvcf-specific logic
  * before handing off to the generic search / persistence services.
  */
class GvcfService(
  persistenceService: PersistenceService,
  searchService: SearchService
)(implicit executionContext: ExecutionContext)
    extends IndexService[GvcfIndex.type, DocumentGvcf](
      persistenceService,
      searchService,
      GvcfIndex,
      ElasticsearchIndex.Gvcf
    )
