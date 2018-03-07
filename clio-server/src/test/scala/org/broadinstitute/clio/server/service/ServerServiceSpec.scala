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
    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)
    serverService.beginStartup()
    succeed
  }

  it should "awaitShutdown" in {
    val app = MockClioApp()
    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)
    serverService.awaitShutdown()
    succeed
  }

  it should "awaitShutdownInf" in {
    val app = MockClioApp()
    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)
    serverService.awaitShutdownInf()
    succeed
  }

  it should "shutdownAndWait" in {
    val app = MockClioApp()
    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)
    serverService.shutdownAndWait()
    succeed
  }

  it should "startup" in {
    val statusDAO = new MemoryServerStatusDAO()
    val persistenceDAO = new MemoryPersistenceDAO()
    val app =
      MockClioApp(serverStatusDAO = statusDAO, persistenceDAO = persistenceDAO)
    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)
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
    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)

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
    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)

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

    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)
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
    val app = MockClioApp()
    val httpServerDAO = new MockHttpServerDAO()
    val serverService = ServerService(app, httpServerDAO)
    for {
      _ <- serverService.shutdown()
    } yield succeed
  }
}
