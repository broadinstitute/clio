package org.broadinstitute.client

import java.security.Permission

import org.scalatest.BeforeAndAfterAll

sealed case class ExitException(status: Int) extends SecurityException("System.exit() is not allowed") {
}

sealed class NoExitSecurityManager extends SecurityManager {
  override def checkPermission(perm: Permission): Unit = {}

  override def checkPermission(perm: Permission, context: Object): Unit = {}

  override def checkExit(status: Int): Unit = {
    super.checkExit(status)
    throw ExitException(status)
  }
}

abstract class SystemExitSpec extends BaseClientSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = System.setSecurityManager(new NoExitSecurityManager())

  override def afterAll(): Unit = System.setSecurityManager(null)
}