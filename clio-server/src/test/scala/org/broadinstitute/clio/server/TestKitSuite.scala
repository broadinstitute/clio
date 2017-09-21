package org.broadinstitute.clio.server

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

/**
  * A mix of Akka TestKit with ScalaTest mixed in to clean up the actor system.
  *
  * @param actorSystemName The name of the actor system.
  */
abstract class TestKitSuite(actorSystemName: String)
    extends TestKit(ActorSystem(actorSystemName))
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  protected implicit val materializer: Materializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    shutdown()
  }
}
