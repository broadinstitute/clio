package org.broadinstitute.clio.server.service

import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{
  FailingPersistenceDAO,
  MemoryPersistenceDAO,
  MemorySearchDAO
}
import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.Location

class PersistenceServiceSpec
    extends TestKitSuite("PersistenceServiceSpec")
    with ModelAutoDerivation {
  behavior of "PersistenceService"

  val mockKey = TransferUbamV1Key(Location.OnPrem, "barcode", 1, "library")
  val mockMetadata = TransferUbamV1Metadata()

  it should "upsertMetadata" in {
    val persistenceDAO = new MemoryPersistenceDAO()
    val searchDAO = new MemorySearchDAO()
    val app =
      MockClioApp(persistenceDAO = persistenceDAO, searchDAO = searchDAO)
    val persistenceService = PersistenceService(app)

    for {
      uuid <- persistenceService.upsertMetadata(
        mockKey,
        mockMetadata,
        WgsUbamService.v1DocumentConverter,
        ElasticsearchIndex.WgsUbam
      )
    } yield {
      val expectedDocument = WgsUbamService.v1DocumentConverter
        .document(mockKey, mockMetadata)
        .deepMerge(
          Map(ElasticsearchIndex.UpsertIdElasticsearchName -> uuid).asJson
        )

      val expectedIndex = ElasticsearchIndex.WgsUbam

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
    val app =
      MockClioApp(persistenceDAO = persistenceDAO, searchDAO = searchDAO)
    val persistenceService = PersistenceService(app)

    recoverToSucceededIf[Exception] {
      persistenceService.upsertMetadata(
        mockKey,
        mockMetadata,
        WgsUbamService.v1DocumentConverter,
        ElasticsearchIndex.WgsUbam
      )
    }.map { _ =>
      searchDAO.updateCalls should be(empty)
    }
  }
}
