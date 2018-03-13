package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata,
  TransferUbamV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsUbamServiceSpec extends IndexServiceSpec[WgsUbamIndex.type]("WgsUbamService") {
  val elasticsearchIndex: ElasticsearchIndex[_] = ElasticsearchIndex.WgsUbam

  val dummyKey = TransferUbamV1Key(Location.GCP, "barcode1", 2, "library3")

  val dummyInput = TransferUbamV1QueryInput(project = Option("testProject"))

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): TransferUbamV1Metadata = {
    TransferUbamV1Metadata(
      project = Option("testProject"),
      notes = Option("notable update"),
      documentStatus = documentStatus
    )
  }

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): WgsUbamService = {
    new WgsUbamService(persistenceService, searchService)
  }
}
