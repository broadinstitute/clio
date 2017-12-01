package org.broadinstitute.clio.client

import java.net.URI

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.TestKit
import org.broadinstitute.clio.client.dispatch.CommandDispatch
import org.broadinstitute.clio.client.util.{IoUtil, MockIoUtil, TestData}
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

abstract class BaseClientSpec
    extends TestKit(ActorSystem("ClioClientSpec"))
    with AsyncFlatSpecLike
    with BeforeAndAfterAll
    with TestData
    with Matchers
    with ModelAutoDerivation {

  def succeedingDispatcher(
    ioUtil: IoUtil = new MockIoUtil,
    jsonToReturn: Option[URI] = None
  ) =
    new CommandDispatch(
      new MockClioWebClient(StatusCodes.OK, jsonToReturn),
      ioUtil
    )

  val failingDispatcher = new CommandDispatch(
    new MockClioWebClient(StatusCodes.InternalServerError, None),
    new MockIoUtil
  )

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
