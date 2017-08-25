package org.broadinstitute.client

import org.broadinstitute.client.util.{MockIoUtil, TestData}
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.CommandDispatch
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterEach, Matchers}

abstract class BaseClientSpec
    extends TestKit(ActorSystem("ClioClientSpec"))
    with AsyncFlatSpecLike
    with TestData
    with Matchers
    with BeforeAndAfterEach {

  val succeedingDispatcher =
    new CommandDispatch(MockClioWebClient.returningOk, MockIoUtil)
  val failingDispatcher =
    new CommandDispatch(MockClioWebClient.returningInternalError, MockIoUtil)

  override protected def beforeEach(): Unit = MockIoUtil.resetMockState()

}
