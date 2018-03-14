package org.broadinstitute.clio.server.service

import java.net.URI

import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class GvcfServiceSpec extends IndexServiceSpec[GvcfIndex.type]("GvcfService") {

  val elasticsearchIndex: ElasticsearchIndex[GvcfIndex.type] = ElasticsearchIndex.Gvcf

  val dummyKey = TransferGvcfV1Key(Location.GCP, "project1", "sample1", 1)

  val dummyInput = TransferGvcfV1QueryInput(project = Option("testProject"))

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): TransferGvcfV1Metadata = {
    TransferGvcfV1Metadata(
      gvcfPath = Option(
        URI.create(s"gs://path/gvcfPath${GvcfExtensions.GvcfExtension}")
      ),
      notes = Option("notable update"),
      documentStatus = documentStatus
    )
  }

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): GvcfService = {
    new GvcfService(persistenceService, searchService)
  }
}
