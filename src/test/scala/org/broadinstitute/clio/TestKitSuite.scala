package org.broadinstitute.clio

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * A mix of Akka TestKit with ScalaTest mixed in to clean up the actor system.
  *
  * @param actorSystemName The name of the actor system.
  */
abstract class TestKitSuite(actorSystemName: String) extends TestKit(ActorSystem(actorSystemName))
  with Suite with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    shutdown()
  }
}
