package org.broadinstitute.clio.server.service

import java.net.URI

import com.sksamuel.elastic4s.searches.queries.{
  BoolQueryDefinition,
  QueryStringQueryDefinition
}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  GvcfKey,
  GvcfMetadata,
  GvcfQueryInput
}
import org.broadinstitute.clio.util.model.{DataType, DocumentStatus, Location}

class GvcfServiceSpec extends IndexServiceSpec[GvcfIndex.type]("GvcfService") {

  val elasticsearchIndex: ElasticsearchIndex[GvcfIndex.type] = ElasticsearchIndex.Gvcf

  val dummyKey = GvcfKey(Location.GCP, "project1", DataType.WGS, "sample1", 1)

  val dummyInput = GvcfQueryInput(project = Option("testProject"))

  val dummyKeyQuery = BoolQueryDefinition(
    must = Seq(
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.location.toString + "\"",
        defaultField = Some("location")
      ),
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.project + "\"",
        defaultField = Some("project.exact")
      ),
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.sampleAlias + "\"",
        defaultField = Some("sample_alias.exact")
      ),
      QueryStringQueryDefinition(
        query = "\"" + dummyKey.version.toString + "\"",
        defaultField = Some("version")
      )
    )
  )

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

  def copyDummyMetadataChangeField(metadata: GvcfMetadata): GvcfMetadata = {
    metadata.copy(notes = Some(randomString))
  }

  def getService(
    persistenceDAO: PersistenceDAO,
    searchDAO: SearchDAO
  ): GvcfService = {
    new GvcfService(persistenceDAO, searchDAO)
  }
}
