package org.broadinstitute.clio.server.service

import com.sksamuel.elastic4s.searches.queries.{
  BoolQueryDefinition,
  QueryStringQueryDefinition
}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
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

  val dummyKeyQuery = BoolQueryDefinition(
    must = Seq(
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.chipwellBarcode.name + "\"",
        defaultField = Some("chipwell_barcode")
      ),
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.location.toString + "\"",
        defaultField = Some("location")
      ),
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.version.toString + "\"",
        defaultField = Some("version")
      )
    )
  )

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
    persistenceDAO: PersistenceDAO,
    searchDAO: SearchDAO
  ): ArraysService = {
    new ArraysService(persistenceDAO, searchDAO)
  }
}
