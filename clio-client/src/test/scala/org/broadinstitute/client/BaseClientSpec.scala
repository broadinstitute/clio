package org.broadinstitute.client

import org.broadinstitute.client.util.{MockIoUtil, TestData}
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.CommandDispatch
import org.scalatest.{AsyncFlatSpecLike, Matchers}

abstract class BaseClientSpec
    extends TestKit(ActorSystem("ClioClientSpec"))
    with AsyncFlatSpecLike
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
}
