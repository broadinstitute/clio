package org.broadinstitute.clio.client.webclient

import java.time.{Instant, OffsetDateTime}
import java.util.Date

import com.google.auth.oauth2.{AccessToken, OAuth2Credentials}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class GoogleCredentialsGeneratorSpec extends FlatSpec with Matchers {

  import GoogleCredentialsGeneratorSpec.MockOAuth2Credentials

  behavior of "GoogleTokenGenerator"

  it should "refresh uninitialized OAuth2Credentials" in {
    val mockCreds = new MockOAuth2Credentials(None)
    val generator = new GoogleCredentialsGenerator(mockCreds)

    mockCreds.getAccessToken should be(null)
    mockCreds.getAccessToken should be(null)
    val token = generator.generateCredentials()
    mockCreds.getAccessToken shouldNot be(null)
    mockCreds.getAccessToken.getTokenValue should be(token.token)
  }

  it should "refresh expired OAuth2Credentials" in {
    val oldTokenString = "abcdefg"

    val oldToken =
      new AccessToken(oldTokenString, Date.from(Instant.now().minusSeconds(10)))
    val mockCreds = new MockOAuth2Credentials(Some(oldToken))
    val generator = new GoogleCredentialsGenerator(mockCreds)

    generator.generateCredentials().token shouldNot be(oldTokenString)
    mockCreds.getAccessToken shouldNot be(oldToken)
  }

  it should "not refresh unexpired OAuth2Credentials" in {
    val oldTokenString = "abcdefg"

    val oldToken =
      new AccessToken(oldTokenString, Date.from(Instant.now().plusSeconds(10)))
    val mockCreds = new MockOAuth2Credentials(Some(oldToken))
    val generator = new GoogleCredentialsGenerator(mockCreds)

    generator.generateCredentials().token should be(oldTokenString)
    mockCreds.getAccessToken should be(oldToken)
  }
}

object GoogleCredentialsGeneratorSpec {
  class MockOAuth2Credentials(initToken: Option[AccessToken])
      extends OAuth2Credentials(initToken.orNull) {

    override def refreshAccessToken(): AccessToken = {
      val newToken = Random.nextString(20)
      val newExpiration =
        Date.from(OffsetDateTime.now().plusSeconds(1).toInstant)

      new AccessToken(newToken, newExpiration)
    }
  }
}
