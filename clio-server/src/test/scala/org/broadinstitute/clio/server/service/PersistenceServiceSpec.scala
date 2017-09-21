package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import Elastic4sAutoDerivation._
import org.broadinstitute.clio.server.dataaccess.{
  FailingPersistenceDAO,
  MemoryPersistenceDAO,
  MemorySearchDAO
}
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata
}
import org.broadinstitute.clio.util.model.Location

import com.sksamuel.elastic4s.circe._

import scala.concurrent.Future

class PersistenceServiceSpec extends TestKitSuite("PersistenceServiceSpec") {
  behavior of "PersistenceService"

  val mockKey = TransferWgsUbamV1Key("barcode", 1, "library", Location.OnPrem)
  val mockMetadata = TransferWgsUbamV1Metadata()

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
        ElasticsearchIndex.WgsUbam,
        WgsUbamService.v1DocumentConverter
      )
    } yield {
      val expectedDocument =
        WgsUbamService.v1DocumentConverter.empty(mockKey).copy(upsertId = uuid)
      persistenceDAO.writeCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.WgsUbam))
      )
      searchDAO.updateCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.WgsUbam))
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
        ElasticsearchIndex.WgsUbam,
        WgsUbamService.v1DocumentConverter
      )
    }.map { _ =>
      searchDAO.updateCalls should be(empty)
    }
  }

  it should "recover metadata from storage" in {
    val persistenceDAO = new MemoryPersistenceDAO()
    val searchDAO = new MemorySearchDAO()
    val app =
      MockClioApp(persistenceDAO = persistenceDAO, searchDAO = searchDAO)

    val numDocs = 1000
    val initInSearch = numDocs / 2

    val persistenceService = PersistenceService(app)
    val initStoredDocuments = Seq.fill(numDocs)(DocumentMock.default)
    val initSearchDocuments = initStoredDocuments.take(initInSearch)

    for {
      _ <- Future.sequence(
        initStoredDocuments
          .map(persistenceDAO.writeUpdate(_, DocumentMock.index))
      )
      _ <- Future.sequence(
        initSearchDocuments.map(searchDAO.updateMetadata(_, DocumentMock.index))
      )
      numRestored <- persistenceService.recoverMetadata(DocumentMock.index)
    } yield {
      numRestored should be(numDocs - initInSearch)
      searchDAO.updateCalls should be(
        initStoredDocuments.map((_, DocumentMock.index))
      )
    }
  }
}
