package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentMock
import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.status.model.ClioStatus

import scala.concurrent.Future

class ServerServiceSpec extends TestKitSuite("ServerServiceSpec") {
  behavior of "ServerService"

  it should "beginStartup" in {
    val app = MockClioApp()
    val serverService = ServerService(app)
    serverService.beginStartup()
    succeed
  }

  it should "awaitShutdown" in {
    val app = MockClioApp()
    val serverService = ServerService(app)
    serverService.awaitShutdown()
    succeed
  }

  it should "awaitShutdownInf" in {
    val app = MockClioApp()
    val serverService = ServerService(app)
    serverService.awaitShutdownInf()
    succeed
  }

  it should "shutdownAndWait" in {
    val app = MockClioApp()
    val serverService = ServerService(app)
    serverService.shutdownAndWait()
    succeed
  }

  it should "startup" in {
    val statusDAO = new MemoryServerStatusDAO()
    val persistenceDAO = new MemoryPersistenceDAO()
    val app =
      MockClioApp(serverStatusDAO = statusDAO, persistenceDAO = persistenceDAO)
    val serverService = ServerService(app)
    serverService.startup().map { _ =>
      statusDAO.setCalls should be(
        Seq(ClioStatus.Starting, ClioStatus.Recovering, ClioStatus.Started)
      )
    }
  }

  it should "fail to start if persistence initialization fails" in {
    val statusDAO = new MemoryServerStatusDAO()
    val app = MockClioApp(
      persistenceDAO = new FailingPersistenceDAO(),
      serverStatusDAO = statusDAO
    )
    val serverService = ServerService(app)

    recoverToSucceededIf[Exception] {
      serverService.startup()
    }.map { _ =>
      statusDAO.setCalls should be(Seq(ClioStatus.Starting))
    }
  }

  it should "fail to start if search initialization fails" in {
    val statusDAO = new MemoryServerStatusDAO()
    val app = MockClioApp(
      searchDAO = new FailingSearchDAO(),
      serverStatusDAO = statusDAO
    )
    val serverService = ServerService(app)

    recoverToSucceededIf[Exception] {
      serverService.startup()
    }.map { _ =>
      statusDAO.setCalls should be(Seq(ClioStatus.Starting))
    }
  }

  it should "recover metadata from storage" in {
    val persistenceDAO = new MemoryPersistenceDAO()
    val searchDAO = new MemorySearchDAO()
    val app =
      MockClioApp(persistenceDAO = persistenceDAO, searchDAO = searchDAO)

    val numDocs = 1000
    val initInSearch = numDocs / 2

    val serverService = ServerService(app)
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
      numRestored <- serverService.recoverMetadata(DocumentMock.index)
    } yield {
      numRestored should be(numDocs - initInSearch)
      searchDAO.updateCalls.map {
        _._1.as[DocumentMock](DocumentMock.index.decoder).fold(throw _, identity),
      } should be(initStoredDocuments)
    }
  }

  it should "shutdown" in {
    val app = MockClioApp()
    val serverService = ServerService(app)
    for {
      _ <- serverService.shutdown()
    } yield succeed
  }
}
