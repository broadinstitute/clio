package org.broadinstitute.clio.util.auth

import java.time.{Duration, OffsetDateTime}
import java.time.temporal.{ChronoUnit, TemporalAmount}
import java.util.Date

import com.google.auth.oauth2.{AccessToken, OAuth2Credentials}

import scala.sys.process.Process

/**
  * OAuth2Credentials wrapper that implements token refresh by
  * shelling out to gcloud.
  */
object ExternalGCloudCredentials extends OAuth2Credentials {

  /**
    * gcloud command to print a fresh access token.
    */
  val RefreshCommand: Seq[String] = Seq("gcloud", "auth", "print-access-token")

  /**
    * Time-to-live for tokens produced by shelling out to gcloud.
    */
  val TokenTtl: TemporalAmount = Duration.of(1, ChronoUnit.HOURS)

  override def refreshAccessToken(): AccessToken = {
    val tokenExpirationTime =
      Date.from(OffsetDateTime.now().plus(TokenTtl).toInstant)
    val stringToken = Process(RefreshCommand).!!.trim

    new AccessToken(stringToken, tokenExpirationTime)
  }
}
