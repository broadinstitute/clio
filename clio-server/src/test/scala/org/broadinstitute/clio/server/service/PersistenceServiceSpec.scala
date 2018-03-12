package org.broadinstitute.clio.server.service

import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  AutoElasticsearchDocumentMapper,
  DocumentWgsUbam,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.{
  FailingPersistenceDAO,
  MemoryPersistenceDAO,
  MemorySearchDAO
}
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata
}
import org.broadinstitute.clio.util.model.Location

class PersistenceServiceSpec extends TestKitSuite("PersistenceServiceSpec") {
  behavior of "PersistenceService"

  val mockKey = TransferUbamV1Key(Location.OnPrem, "barcode", 1, "library")
  val mockMetadata = TransferUbamV1Metadata()

  val mockDocConverter = AutoElasticsearchDocumentMapper[
    TransferUbamV1Key,
    TransferUbamV1Metadata,
    DocumentWgsUbam
  ]

  it should "upsertMetadata" in {
    val persistenceDAO = new MemoryPersistenceDAO()
    val searchDAO = new MemorySearchDAO()
    val persistenceService = PersistenceService(persistenceDAO, searchDAO)

    for {
      uuid <- persistenceService.upsertMetadata(
        mockKey,
        mockMetadata,
        mockDocConverter
      )
    } yield {
      val expectedDocument = mockDocConverter
        .empty(mockKey)
        .copy(upsertId = uuid)

      val expectedIndex = ElasticsearchIndex.WgsUbam

      persistenceDAO.writeCalls should be(
        Seq((expectedDocument, expectedIndex))
      )
      searchDAO.updateCalls.flatMap { case (jsons, index) => jsons.map(_ -> index) } should be(
        Seq((expectedDocument.asJson(expectedIndex.encoder), expectedIndex))
      )
    }
  }

  it should "not update search if writing to storage fails" in {
    val persistenceDAO = new FailingPersistenceDAO()
    val searchDAO = new MemorySearchDAO()
    val persistenceService = PersistenceService(persistenceDAO, searchDAO)

    recoverToSucceededIf[Exception] {
      persistenceService.upsertMetadata(
        mockKey,
        mockMetadata,
        mockDocConverter
      )
    }.map { _ =>
      searchDAO.updateCalls should be(empty)
    }
  }
}
