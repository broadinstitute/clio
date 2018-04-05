package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata, UbamQueryInput}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsUbamServiceSpec extends IndexServiceSpec[WgsUbamIndex.type]("WgsUbamService") {

  val elasticsearchIndex: ElasticsearchIndex[WgsUbamIndex.type] =
    ElasticsearchIndex.WgsUbam

  val dummyKey = UbamKey(Location.GCP, "barcode1", 2, "library3")

  val dummyInput = UbamQueryInput(project = Option("testProject"))

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): UbamMetadata = {
    UbamMetadata(
      project = Option("testProject"),
      notes = Option("notable update"),
      documentStatus = documentStatus
    )
  }

  def copyDummyMetadataChangeField(metadata: UbamMetadata): UbamMetadata = {
    metadata.copy(project = Some(util.Random.nextString(10)))
  }

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): WgsUbamService = {
    new WgsUbamService(persistenceService, searchService)
  }
}
