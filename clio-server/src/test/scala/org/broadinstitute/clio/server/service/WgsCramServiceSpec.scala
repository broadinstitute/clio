package org.broadinstitute.clio.server.service

import java.net.URI

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput,
  WgsCramExtensions
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsCramServiceSpec extends IndexServiceSpec[WgsCramIndex.type]("WgsCramService") {

  val elasticsearchIndex: ElasticsearchIndex[_] = ElasticsearchIndex.WgsCram

  val dummyKey = TransferWgsCramV1Key(Location.GCP, "project1", "sample1", 1)

  val dummyInput = TransferWgsCramV1QueryInput(project = Option("testProject"))

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): TransferWgsCramV1Metadata = {
    TransferWgsCramV1Metadata(
      cramPath = Option(
        URI.create(s"gs://path/cramPath${WgsCramExtensions.CramExtension}")
      ),
      notes = Option("notable update"),
      documentStatus = documentStatus
    )
  }

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): WgsCramService = {
    new WgsCramService(persistenceService, searchService)
  }
}
