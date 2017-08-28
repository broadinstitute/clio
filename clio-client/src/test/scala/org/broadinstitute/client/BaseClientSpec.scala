package org.broadinstitute.client

import org.broadinstitute.client.util.{MockIoUtil, TestData}
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
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
    new CommandDispatch(MockClioWebClient.returningOk, MockIoUtil)
  val succeedingDispatcher = new CommandDispatch(
    new MockClioWebClient(StatusCodes.OK, snakeCaseMetadataFileLocation.get),
    MockIoUtil
  )
  val failingDispatcher =
    new CommandDispatch(MockClioWebClient.returningInternalError, MockIoUtil)
}
