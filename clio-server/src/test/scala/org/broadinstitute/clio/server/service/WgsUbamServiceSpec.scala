package org.broadinstitute.clio.server.service

import com.sksamuel.elastic4s.searches.queries.{
  BoolQueryDefinition,
  QueryStringQueryDefinition
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata, UbamQueryInput}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsUbamServiceSpec extends IndexServiceSpec[WgsUbamIndex.type]("WgsUbamService") {

  val elasticsearchIndex: ElasticsearchIndex[WgsUbamIndex.type] =
    ElasticsearchIndex.WgsUbam

  val dummyKey = UbamKey(Location.GCP, "barcode1", 2, "library3")

  val dummyInput = UbamQueryInput(project = Option("testProject"))

  val dummyKeyQuery = BoolQueryDefinition(
    must = Seq(
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.flowcellBarcode + "\"",
        defaultField = Some("flowcell_barcode.exact")
      ),
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.lane.toString + "\"",
        defaultField = Some("lane")
      ),
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.libraryName + "\"",
        defaultField = Some("library_name.exact")
      ),
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.location.toString + "\"",
        defaultField = Some("location")
      )
    )
  )

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
    metadata.copy(project = Some(randomString))
  }

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): WgsUbamService = {
    new WgsUbamService(persistenceService, searchService)
  }
}
