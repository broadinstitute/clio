package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.{
  ArraysKey,
  ArraysMetadata,
  ArraysQueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class ArraysServiceSpec extends IndexServiceSpec[ArraysIndex.type]("ArraysService") {

  val elasticsearchIndex: ElasticsearchIndex[ArraysIndex.type] =
    ElasticsearchIndex.Arrays

  val dummyKey = ArraysKey(Location.GCP, Symbol("chipwell_barcode"), 1)

  val dummyInput = ArraysQueryInput(project = Option("testProject"))

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): ArraysMetadata = {
    ArraysMetadata(
      project = Option("testProject"),
      notes = Option("notable update"),
      documentStatus = documentStatus
    )
  }

  def copyDummyMetadataChangeField(metadata: ArraysMetadata): ArraysMetadata = {
    metadata.copy(project = Some(randomString))
  }

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): ArraysService = {
    new ArraysService(persistenceService, searchService)
  }
}
