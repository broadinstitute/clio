package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.{
  FailingPersistenceDAO,
  FailingSearchDAO,
  MemoryServerStatusDAO
}
import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.status.model.ServerStatusInfo

import org.scalatest.{AsyncFlatSpecLike, Matchers}

class ServerServiceSpec
    extends TestKitSuite("ServerServiceSpec")
    with AsyncFlatSpecLike
    with Matchers {
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
    val app = MockClioApp(serverStatusDAO = statusDAO)
    val serverService = ServerService(app)
    for {
      _ <- serverService.beginStartup()
    } yield {
      statusDAO.setCalls should be(Seq(ServerStatusInfo.Starting))
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
      serverService.beginStartup()
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
      serverService.beginStartup()
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
