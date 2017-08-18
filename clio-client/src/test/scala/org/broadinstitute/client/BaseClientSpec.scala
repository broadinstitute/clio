package org.broadinstitute.client

import org.broadinstitute.client.util.TestData

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{AsyncFlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext

abstract class BaseClientSpec
    extends TestKit(ActorSystem("ClioClientSpec"))
    with AsyncFlatSpecLike
    with TestData
    with Matchers {

  /*
   * Since our Commands call Await.result, we can't use the default
   * execution context provided by Scalatest's async test suite, otherwise
   * the blocking will prevent our tests from ever completing.
   *
   * See the "Asynchronous execution model" section of
   * http://www.scalatest.org/user_guide/async_testing for more info.
   */
  override implicit val executionContext: ExecutionContext = system.dispatcher
}
