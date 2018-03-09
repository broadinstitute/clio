package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentWgsUbam,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata,
  TransferUbamV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}

class WgsUbamServiceSpec extends TestKitSuite("WgsUbamServiceSpec") {
  val index: ElasticsearchIndex[DocumentWgsUbam] = ElasticsearchIndex.WgsUbam

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

  def copyDocumentWithUpsertId(
    originalDocument: DocumentWgsUbam,
    upsertId: UpsertId
  ): DocumentWgsUbam = {
    originalDocument.copy(
      upsertId = upsertId
    )
  }
}
