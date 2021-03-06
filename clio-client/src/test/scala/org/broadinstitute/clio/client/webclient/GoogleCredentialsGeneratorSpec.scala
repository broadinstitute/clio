package org.broadinstitute.clio.client.webclient

import java.time.OffsetDateTime
import java.util.Date

import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.util.Random

class GoogleCredentialsGeneratorSpec extends FlatSpec with Matchers {

  import GoogleCredentialsGeneratorSpec.MockOAuth2Credentials

  behavior of "GoogleTokenGenerator"

  it should "refresh uninitialized OAuth2Credentials" in {
    val mockCreds = new MockOAuth2Credentials()
    val generator = new GoogleCredentialsGenerator(mockCreds)

    mockCreds.getAccessToken should be(null)
    val token = generator.generateCredentials()
    mockCreds.getAccessToken shouldNot be(null)
    mockCreds.getAccessToken.getTokenValue should be(token.token)
  }

  it should "refresh expired OAuth2Credentials" in {
    // Zero duration so each refresh produces an instantly-expired token.
    val mockCreds = new MockOAuth2Credentials(Duration.Zero)
    val generator = new GoogleCredentialsGenerator(mockCreds)

    val token1 = generator.generateCredentials()
    val token2 = generator.generateCredentials()

    token1.token shouldNot be(token2.token)
    mockCreds.getAccessToken.getTokenValue should be(token2.token)
  }

  it should "not refresh unexpired OAuth2Credentials" in {
    val mockCreds = new MockOAuth2Credentials(100.seconds)
    val generator = new GoogleCredentialsGenerator(mockCreds)

    val token1 = generator.generateCredentials()
    val token2 = generator.generateCredentials()

    token1 should be(token2)
  }
}

object GoogleCredentialsGeneratorSpec {

  class MockOAuth2Credentials(tokenDuration: FiniteDuration = 1.second)
      extends GoogleCredentials {

    override def refreshAccessToken(): AccessToken = {
      val newToken = Random.nextString(20)
      val newExpiration =
        Date.from(OffsetDateTime.now().plusNanos(tokenDuration.toNanos).toInstant)

      new AccessToken(newToken, newExpiration)
    }
  }
}
