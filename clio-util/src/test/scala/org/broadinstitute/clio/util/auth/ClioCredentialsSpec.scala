package org.broadinstitute.clio.util.auth

import java.util.Collections

import com.google.auth.oauth2.GoogleCredentials
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

class ClioCredentialsSpec extends FlatSpec with Matchers with MockFactory {
  behavior of "ClioCredentials"

  it should "build user-info credentials" in {
    val mockBase = mock[GoogleCredentials]
    val mockScoped = mock[GoogleCredentials]
    (mockBase.createScoped _).expects(ClioCredentials.userInfoScopes).returning(mockScoped)

    new ClioCredentials(mockBase).userInfo() should be(mockScoped)
  }

  it should "build read-write storage credentials" in {
    val mockBase = mock[GoogleCredentials]
    val mockScoped = mock[GoogleCredentials]
    (mockBase.createScoped _)
      .expects(Collections.singleton(ClioCredentials.readWriteStorageScope))
      .returning(mockScoped)

    new ClioCredentials(mockBase).storage(readOnly = false) should be(mockScoped)
  }

  it should "build read-only storage credentials" in {
    val mockBase = mock[GoogleCredentials]
    val mockScoped = mock[GoogleCredentials]
    (mockBase.createScoped _)
      .expects(Collections.singleton(ClioCredentials.readOnlyStorageScope))
      .returning(mockScoped)

    new ClioCredentials(mockBase).storage(readOnly = true) should be(mockScoped)
  }
}
