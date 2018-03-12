package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentMock
import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.status.model.ClioStatus

import scala.concurrent.Future

class ServerServiceSpec extends TestKitSuite("ServerServiceSpec") {
  behavior of "ServerService"

  def serverServiceWithMockDefaults(
    serverStatusDAO: ServerStatusDAO = new MockServerStatusDAO(),
    persistenceDAO: PersistenceDAO = new MockPersistenceDAO(),
    searchDAO: SearchDAO = new MockSearchDAO(),
    httpServerDAO: HttpServerDAO = new MockHttpServerDAO()
  ) = ServerService(
    serverStatusDAO,
    persistenceDAO,
    searchDAO,
    httpServerDAO
  )

  it should "beginStartup" in {
    val serverService = serverServiceWithMockDefaults()
    serverService.beginStartup()
    succeed
  }

  it should "awaitShutdown" in {
    val serverService = serverServiceWithMockDefaults()
    serverService.awaitShutdown()
    succeed
  }

  it should "awaitShutdownInf" in {
    val serverService = serverServiceWithMockDefaults()
    serverService.awaitShutdownInf()
    succeed
  }

  it should "shutdownAndWait" in {
    val serverService = serverServiceWithMockDefaults()
    serverService.shutdownAndWait()
    succeed
  }

  it should "startup" in {
    val statusDAO = new MemoryServerStatusDAO()
    val persistenceDAO = new MemoryPersistenceDAO()
    val serverService = serverServiceWithMockDefaults(
      serverStatusDAO = statusDAO,
      persistenceDAO = persistenceDAO
    )
    serverService.startup().map { _ =>
      statusDAO.setCalls should be(
        Seq(ClioStatus.Starting, ClioStatus.Recovering, ClioStatus.Started)
      )
    }
  }

  it should "fail to start if persistence initialization fails" in {
    val statusDAO = new MemoryServerStatusDAO()
    val persistenceDAO = new FailingPersistenceDAO()
    val serverService = serverServiceWithMockDefaults(
      serverStatusDAO = statusDAO,
      persistenceDAO = persistenceDAO
    )

    recoverToSucceededIf[Exception] {
      serverService.startup()
    }.map { _ =>
      statusDAO.setCalls should be(Seq(ClioStatus.Starting))
    }
  }

  it should "fail to start if search initialization fails" in {
    val statusDAO = new MemoryServerStatusDAO()
    val searchDAO = new FailingSearchDAO()
    val serverService = serverServiceWithMockDefaults(
      serverStatusDAO = statusDAO,
      searchDAO = searchDAO
    )

    recoverToSucceededIf[Exception] {
      serverService.startup()
    }.map { _ =>
      statusDAO.setCalls should be(Seq(ClioStatus.Starting))
    }
  }

  it should "recover metadata from storage" in {
    val persistenceDAO = new MemoryPersistenceDAO()
    val searchDAO = new MemorySearchDAO()

    val numDocs = 1000
    val initInSearch = numDocs / 2

    val serverService = serverServiceWithMockDefaults(
      persistenceDAO = persistenceDAO,
      searchDAO = searchDAO
    )
    val initStoredDocuments = Seq.fill(numDocs)(DocumentMock.default)
    val initSearchDocuments = initStoredDocuments.take(initInSearch)

    for {
      _ <- persistenceDAO.initialize(Seq(DocumentMock.index), "fake-version")
      _ <- Future.sequence(
        initStoredDocuments.map(persistenceDAO.writeUpdate[DocumentMock](_))
      )
      _ <- Future.sequence(
        initSearchDocuments.map(searchDAO.updateMetadata[DocumentMock])
      )
      _ <- serverService.recoverMetadata(DocumentMock.index)
    } yield {
      val upsertedDocs = searchDAO.updateCalls
        .flatMap(_._1)
        .map(_.as[DocumentMock](DocumentMock.index.decoder).fold(throw _, identity))
      upsertedDocs should contain theSameElementsAs initStoredDocuments
    }
  }

  it should "shutdown" in {
    val serverService = serverServiceWithMockDefaults()
    for {
      _ <- serverService.shutdown()
    } yield succeed
  }
}
