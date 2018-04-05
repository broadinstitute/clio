package org.broadinstitute.clio.server.service

import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchDocumentMapper,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.{
  FailingPersistenceDAO,
  MemoryPersistenceDAO,
  MemorySearchDAO
}
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.Location

class PersistenceServiceSpec
    extends TestKitSuite("PersistenceServiceSpec")
    with ModelAutoDerivation {
  behavior of "PersistenceService"

  val mockKey = UbamKey(Location.OnPrem, "barcode", 1, "library")
  val mockMetadata = UbamMetadata()

  val expectedIndex: ElasticsearchIndex[WgsUbamIndex.type] = ElasticsearchIndex.WgsUbam

  val mockDocConverter: ElasticsearchDocumentMapper[
    UbamKey,
    UbamMetadata
  ] = ElasticsearchDocumentMapper[
    UbamKey,
    UbamMetadata
  ]

  it should "upsertMetadata" in {
    val persistenceDAO = new MemoryPersistenceDAO()
    val searchDAO = new MemorySearchDAO()
    val persistenceService = new PersistenceService(persistenceDAO, searchDAO)

    for {
      uuid <- persistenceService.upsertMetadata(
        mockKey,
        mockMetadata,
        mockDocConverter,
        expectedIndex
      )
    } yield {
      val expectedDocument = mockDocConverter
        .document(mockKey, mockMetadata)
        .deepMerge(
          Map(ElasticsearchIndex.UpsertIdElasticsearchName -> uuid).asJson
        )

      persistenceDAO.writeCalls should be(
        Seq((expectedDocument, expectedIndex))
      )
      searchDAO.updateCalls.flatMap { case (jsons, index) => jsons.map(_ -> index) } should be(
        Seq((expectedDocument, expectedIndex))
      )
    }
  }

  it should "not update search if writing to storage fails" in {
    val persistenceDAO = new FailingPersistenceDAO()
    val searchDAO = new MemorySearchDAO()
    val persistenceService = new PersistenceService(persistenceDAO, searchDAO)

    recoverToSucceededIf[Exception] {
      persistenceService.upsertMetadata(
        mockKey,
        mockMetadata,
        mockDocConverter,
        expectedIndex
      )
    }.map { _ =>
      searchDAO.updateCalls should be(empty)
    }
  }
}
