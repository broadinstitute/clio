package org.broadinstitute.clio.server.service

import java.net.URI

import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  GvcfKey,
  GvcfMetadata,
  GvcfQueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class GvcfServiceSpec extends IndexServiceSpec[GvcfIndex.type]("GvcfService") {

  val elasticsearchIndex: ElasticsearchIndex[GvcfIndex.type] = ElasticsearchIndex.Gvcf

  val dummyKey = GvcfKey(Location.GCP, "project1", "sample1", 1)

  val dummyInput = GvcfQueryInput(project = Option("testProject"))

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): GvcfMetadata = {
    GvcfMetadata(
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
