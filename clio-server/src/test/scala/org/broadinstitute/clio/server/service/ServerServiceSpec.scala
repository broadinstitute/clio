package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
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
    val app = MockClioApp()
    val serverService = ServerService(app)
    for {
      _ <- serverService.startup()
    } yield succeed
  }

  it should "shutdown" in {
    val app = MockClioApp()
    val serverService = ServerService(app)
    for {
      _ <- serverService.shutdown()
    } yield succeed
  }
}
