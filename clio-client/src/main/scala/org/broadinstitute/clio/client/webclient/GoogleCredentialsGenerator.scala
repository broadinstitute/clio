package org.broadinstitute.clio.client.webclient

import java.time.Instant

import akka.http.scaladsl.model.headers.{HttpCredentials, OAuth2BearerToken}
import com.google.auth.oauth2.GoogleCredentials
import org.broadinstitute.clio.util.auth.ClioCredentials

/** Credentials generator which wraps Google-style OAuth2Credentials. */
class GoogleCredentialsGenerator private[webclient] (googleCredentials: GoogleCredentials)
    extends CredentialsGenerator {

  override def generateCredentials(): HttpCredentials = {
    val now = Instant.now()

    synchronized {
      val maybeExpirationInstant = for {
        token <- Option(googleCredentials.getAccessToken)
        expiration <- Option(token.getExpirationTime)
      } yield {
        expiration.toInstant
      }

      /*
       * Token is valid if:
       *   1. It's not null, and
       *   2. Its expiration time hasn't passed
       */
      val tokenIsValid =
        // Pretend the token expires a second early to give some wiggle-room
        maybeExpirationInstant.exists(_.minusSeconds(1).isAfter(now))

      if (!tokenIsValid) {
        googleCredentials.refresh()
      }
    }

    OAuth2BearerToken(googleCredentials.getAccessToken.getTokenValue)
  }
}

object GoogleCredentialsGenerator {

  def apply(baseCredentials: ClioCredentials): GoogleCredentialsGenerator =
    new GoogleCredentialsGenerator(baseCredentials.oauth())
}
