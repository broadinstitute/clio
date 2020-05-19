package org.broadinstitute.clio.server.service

import java.net.URI

import com.sksamuel.elastic4s.searches.queries.{BoolQuery, QueryStringQuery}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.transfer.model.BamIndex
import org.broadinstitute.clio.transfer.model.bam.{
  BamExtensions,
  BamKey,
  BamMetadata,
  BamQueryInput
}
import org.broadinstitute.clio.util.model.{DataType, DocumentStatus, Location}

class BamServiceSpec extends IndexServiceSpec[BamIndex.type]("BamService") {

  val elasticsearchIndex: ElasticsearchIndex[BamIndex.type] =
    ElasticsearchIndex.Bam

  val dummyKey = BamKey(Location.GCP, "project1", DataType.WGS, "sample1", 1)

  val dummyInput = BamQueryInput(project = Option("testProject"))

  val dummyKeyQuery = BoolQuery(
    must = Seq(
      QueryStringQuery(
        query = "\"" + dummyKey.location.toString + "\"",
        defaultField = Some("location")
      ),
      QueryStringQuery(
        query = "\"" + dummyKey.project + "\"",
        defaultField = Some("project.exact")
      ),
      QueryStringQuery(
        query = "\"" + dummyKey.sampleAlias + "\"",
        defaultField = Some("sample_alias.exact")
      ),
      QueryStringQuery(
        query = "\"" + dummyKey.version.toString + "\"",
        defaultField = Some("version")
      )
    )
  )

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): BamMetadata = {
    BamMetadata(
      bamPath = Option(
        URI.create(s"gs://path/bamPath${BamExtensions.BamExtension}")
      ),
      notes = Option("notable update"),
      documentStatus = documentStatus,
      workspaceName = Option("the workspace")
    )
  }

  def copyDummyMetadataChangeField(metadata: BamMetadata): BamMetadata = {
    metadata.copy(workspaceName = Some(randomString))
  }

  def copyDummyMetadataSetNullField(metadata: BamMetadata): BamMetadata =
    metadata.copy(bamSize = Some(1L))

  def getService(
    persistenceDAO: PersistenceDAO,
    searchDAO: SearchDAO
  ): BamService = {
    new BamService(persistenceDAO, searchDAO)
  }

}
