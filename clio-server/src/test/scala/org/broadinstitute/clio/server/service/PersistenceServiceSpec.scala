package org.broadinstitute.clio.server.service

import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
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
import org.broadinstitute.clio.util.model.{Location, UpsertId}

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

    persistenceService
      .upsertMetadata(
        mockKey,
        mockMetadata,
        mockDocConverter,
        expectedIndex
      )
      .runWith(Sink.head[UpsertId])
      .map { uuid =>
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

    persistenceService
      .upsertMetadata(
        mockKey,
        mockMetadata,
        mockDocConverter,
        expectedIndex
      )
      .runWith(TestSink.probe[UpsertId])
      .expectSubscriptionAndError()

    searchDAO.updateCalls should be(empty)

  }
}
