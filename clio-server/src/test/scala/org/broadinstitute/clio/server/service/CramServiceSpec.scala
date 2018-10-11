package org.broadinstitute.clio.server.service

import java.net.URI

import com.sksamuel.elastic4s.searches.queries.{
  BoolQueryDefinition,
  QueryStringQueryDefinition
}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.CramIndex
import org.broadinstitute.clio.transfer.model.cram.{
  CramExtensions,
  CramKey,
  CramMetadata,
  CramQueryInput
}
import org.broadinstitute.clio.util.model.{DataType, DocumentStatus, Location}

class CramServiceSpec extends IndexServiceSpec[CramIndex.type]("CramService") {

  val elasticsearchIndex: ElasticsearchIndex[CramIndex.type] =
    ElasticsearchIndex.Cram

  val dummyKey = CramKey(Location.GCP, "project1", DataType.WGS, "sample1", 1)

  val dummyInput = CramQueryInput(project = Option("testProject"))

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
  ): CramMetadata = {
    CramMetadata(
      cramPath = Option(
        URI.create(s"gs://path/cramPath${CramExtensions.CramExtension}")
      ),
      notes = Option("notable update"),
      documentStatus = documentStatus,
      workspaceName = Option("the workspace")
    )
  }

  def copyDummyMetadataChangeField(metadata: CramMetadata): CramMetadata = {
    metadata.copy(workspaceName = Some(randomString))
  }

  def getService(
    persistenceDAO: PersistenceDAO,
    searchDAO: SearchDAO
  ): CramService = {
    new CramService(persistenceDAO, searchDAO)
  }
}
