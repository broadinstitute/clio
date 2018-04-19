package org.broadinstitute.clio.server.service

import com.sksamuel.elastic4s.searches.queries.{
  BoolQueryDefinition,
  QueryStringQueryDefinition
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.UbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata, UbamQueryInput}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class UbamServiceSpec extends IndexServiceSpec[UbamIndex.type]("UbamService") {

  val elasticsearchIndex: ElasticsearchIndex[UbamIndex.type] =
    ElasticsearchIndex.Ubam

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
  ): UbamService = {
    new UbamService(persistenceService, searchService)
  }
}
