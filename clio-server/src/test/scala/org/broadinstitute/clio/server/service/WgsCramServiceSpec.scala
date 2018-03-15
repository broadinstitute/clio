package org.broadinstitute.clio.server.service

import java.net.URI

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  WgsCramKey,
  WgsCramMetadata,
  WgsCramQueryInput,
  WgsCramExtensions
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsCramServiceSpec extends IndexServiceSpec[WgsCramIndex.type]("WgsCramService") {

  val elasticsearchIndex: ElasticsearchIndex[WgsCramIndex.type] =
    ElasticsearchIndex.WgsCram

  val dummyKey = WgsCramKey(Location.GCP, "project1", "sample1", 1)

  val dummyInput = WgsCramQueryInput(project = Option("testProject"))

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): WgsCramMetadata = {
    WgsCramMetadata(
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
