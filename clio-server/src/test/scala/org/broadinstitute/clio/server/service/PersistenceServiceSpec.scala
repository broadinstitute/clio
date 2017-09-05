package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.MockClioApp
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
import org.scalatest.{AsyncFlatSpec, Matchers}

class PersistenceServiceSpec extends AsyncFlatSpec with Matchers {
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
        WgsUbamService.v1DocumentConverter.empty(mockKey).copy(clioId = uuid)
      persistenceDAO.writeCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.WgsUbam))
      )
      searchDAO.updateCalls should be(
        Seq(
          (
            "barcode.1.library.OnPrem",
            expectedDocument,
            ElasticsearchIndex.WgsUbam
          )
        )
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
}
