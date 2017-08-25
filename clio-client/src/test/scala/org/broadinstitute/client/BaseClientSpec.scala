package org.broadinstitute.client

import org.broadinstitute.client.util.{MockIoUtil, TestData}
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.clio.client.util.IoUtil
import org.scalatest.{AsyncFlatSpecLike, Matchers}

abstract class BaseClientSpec
    extends TestKit(ActorSystem("ClioClientSpec"))
    with AsyncFlatSpecLike
    with TestData
    with Matchers {

  implicit val ioUtil: IoUtil = MockIoUtil
}
