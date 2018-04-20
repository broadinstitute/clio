package org.broadinstitute.clio.server.service

import java.net.URI

import com.sksamuel.elastic4s.searches.queries.{
  BoolQueryDefinition,
  QueryStringQueryDefinition
}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  WgsCramExtensions,
  WgsCramKey,
  WgsCramMetadata,
  WgsCramQueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsCramServiceSpec extends IndexServiceSpec[WgsCramIndex.type]("WgsCramService") {

  val elasticsearchIndex: ElasticsearchIndex[WgsCramIndex.type] =
    ElasticsearchIndex.WgsCram

  val dummyKey = WgsCramKey(Location.GCP, "project1", "sample1", 1)

  val dummyInput = WgsCramQueryInput(project = Option("testProject"))

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
  ): WgsCramMetadata = {
    WgsCramMetadata(
      cramPath = Option(
        URI.create(s"gs://path/cramPath${WgsCramExtensions.CramExtension}")
      ),
      notes = Option("notable update"),
      documentStatus = documentStatus,
      workspaceName = Option("the workspace")
    )
  }

  def copyDummyMetadataChangeField(metadata: WgsCramMetadata): WgsCramMetadata = {
    metadata.copy(workspaceName = Some(randomString))
  }

  def getService(
    persistenceDAO: PersistenceDAO,
    searchDAO: SearchDAO
  ): WgsCramService = {
    new WgsCramService(persistenceDAO, searchDAO)
  }
}
