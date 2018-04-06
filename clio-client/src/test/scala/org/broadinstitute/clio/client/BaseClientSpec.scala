package org.broadinstitute.clio.client

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.broadinstitute.clio.client.util.TestData
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

abstract class BaseClientSpec
    extends TestKit(ActorSystem("ClioClientSpec"))
    with AsyncFlatSpecLike
    with BeforeAndAfterAll
    with TestData
    with Matchers
    with ModelAutoDerivation {

  implicit val mat: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
