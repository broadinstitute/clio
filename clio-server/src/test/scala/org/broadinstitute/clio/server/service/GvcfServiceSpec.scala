package org.broadinstitute.clio.server.service

import java.net.URI

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentGvcf,
  ElasticsearchIndex
}
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}

class GvcfServiceSpec
    extends IndexServiceSpec[GvcfIndex.type, DocumentGvcf]("GvcfService") {

  val index: ElasticsearchIndex[DocumentGvcf] = ElasticsearchIndex.Gvcf

  val dummyKey = TransferGvcfV1Key(Location.GCP, "project1", "sample1", 1)

  val dummyInput = TransferGvcfV1QueryInput(project = Option("testProject"))

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): TransferGvcfV1Metadata = {
    TransferGvcfV1Metadata(
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

  def copyDocumentWithUpsertId(
    originalDocument: DocumentGvcf,
    upsertId: UpsertId
  ): DocumentGvcf = {
    originalDocument.copy(
      upsertId = upsertId
    )
  }

  def documentToJson(document: DocumentGvcf): Json = {
    document.asJson(index.encoder)
  }
}
