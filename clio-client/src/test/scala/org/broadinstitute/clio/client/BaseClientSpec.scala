package org.broadinstitute.clio.client

import org.broadinstitute.clio.client.dispatch.CommandDispatch
import org.broadinstitute.clio.client.util.{MockIoUtil, TestData}
import org.broadinstitute.clio.client.webclient.MockClioWebClient

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

abstract class BaseClientSpec
    extends TestKit(ActorSystem("ClioClientSpec"))
    with AsyncFlatSpecLike
    with BeforeAndAfterAll
    with TestData
    with Matchers {

  val succeedingDispatcherCamel =
    new CommandDispatch(MockClioWebClient.returningOk, new MockIoUtil)
  val succeedingDispatcher =
    new CommandDispatch(MockClioWebClient.returningOk, new MockIoUtil)
  def succeedingReturningDispatcher(mockIoUtil: MockIoUtil) =
    new CommandDispatch(MockClioWebClient.returningWgsUbam, mockIoUtil)
  val failingDispatcher =
    new CommandDispatch(
      MockClioWebClient.returningInternalError,
      new MockIoUtil
    )

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}