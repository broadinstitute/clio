package org.broadinstitute.clio.util.auth

import better.files.File
import cats.syntax.either._
import com.google.auth.oauth2.{AccessToken, GoogleCredentials, OAuth2Credentials}
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser.decode
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import scala.util.Try

object AuthUtil extends ModelAutoDerivation with LazyLogging {

  /** Scopes needed from Google to get past Clio's auth proxy. */
  private val authScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  )

  private lazy val shellOutCreds: Either[Throwable, OAuth2Credentials] = {
    logger.debug("Shelling out to gcloud to get credentials")
    // Need to test out the gcloud command here before we build the credentials
    // Since the access token is lazily evaluated.
    Either.catchNonFatal {
      val _ = ExternalGCloudCredentials.refreshAccessToken()
      ExternalGCloudCredentials
    }
  }

  def getCredentials(
    accessToken: Option[AccessToken],
    serviceAccountJson: Option[File]
  ): Either[Throwable, OAuth2Credentials] = {
    Either
      .fromOption(accessToken, new RuntimeException("No access token provided"))
      .flatMap(token => Either.catchNonFatal(GoogleCredentials.create(token)))
      .orElse(getOAuth2Credentials(serviceAccountJson))
      .orElse {
        logger.debug("Falling back to application default credentials")
        Either.catchNonFatal(GoogleCredentials.getApplicationDefault)
      }
  }

  /**
    * Get OAuth2 credentials either from the service account json or
    * from the user's default setup.
    *
    * @param serviceAccountPath Option of path to the service account json
    */
  def getOAuth2Credentials(
    serviceAccountPath: Option[File]
  ): Either[Throwable, OAuth2Credentials] = {

    serviceAccountPath
      .fold(shellOutCreds) { jsonPath =>
        loadServiceAccountJson(jsonPath).fold(
          err => {
            logger.warn(
              s"Failed to load service account JSON at $jsonPath, falling back to gcloud",
              err
            )
            shellOutCreds
          },
          getCredsFromServiceAccount
        )
      }
  }

  /** Load JSON for a google service account. */
  def loadServiceAccountJson(
    serviceAccountPath: File
  ): Either[Throwable, ServiceAccount] = {
    for {
      jsonPath <- Either.cond(
        serviceAccountPath.exists,
        serviceAccountPath, {
          new RuntimeException(
            s"Could not find service account JSON at $serviceAccountPath"
          )
        }
      )
      json <- Try(jsonPath.contentAsString.stripMargin).toEither
      account <- decode[ServiceAccount](json)
    } yield {
      account
    }
  }

  /**
    * Request an access token for the given service account with the
    * scopes needed to talk through Clio's OpenIDC proxy.
    */
  def getCredsFromServiceAccount(
    serviceAccount: ServiceAccount
  ): Either[Throwable, OAuth2Credentials] = {
    logger.debug(
      "Getting credentials from the command-line provided service-account-json"
    )
    serviceAccount.credentialForScopes(authScopes).toEither
  }
}
