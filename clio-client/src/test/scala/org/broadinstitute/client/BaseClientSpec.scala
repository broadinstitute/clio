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

  override implicit val executionContext: ExecutionContext = system.dispatcher
}
