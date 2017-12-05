package org.broadinstitute.clio.util.auth

import java.nio.file.{Files, Path}

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

  def getCredentials(
    accessToken: Option[AccessToken],
    serviceAccountJson: Option[Path]
  ): Either[Throwable, OAuth2Credentials] = {
    accessToken
      .map(new GoogleCredentials(_))
      .fold(getOAuth2Credentials(serviceAccountJson)) { creds =>
        logger.info("Using provided access token as authentication")
        Right(creds)
      }
  }

  /**
    * Get OAuth2 credentials either from the service account json or
    * from the user's default setup.
    *
    * @param serviceAccountPath Option of path to the service account json
    */
  def getOAuth2Credentials(
    serviceAccountPath: Option[Path]
  ): Either[Throwable, OAuth2Credentials] = {

    val shellOutCreds: Either[Throwable, OAuth2Credentials] = {
      logger.info("Shelling out to gcloud to get credentials")
      Right(ExternalGCloudCredentials)
    }

    // Have to go from try to either since either doesn't have an 'orElse' method
    Try(GoogleCredentials.getApplicationDefault)
        .map { gc =>
          logger.info("Using application default credentials")
          gc
        }
      .orElse(
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
          .toTry
      )
      .toEither
  }

  /** Load JSON for a google service account. */
  def loadServiceAccountJson(
    serviceAccountPath: Path
  ): Either[Throwable, ServiceAccount] = {
    for {
      jsonPath <- Either.cond(
        Files.exists(serviceAccountPath),
        serviceAccountPath, {
          val message = s"Could not find service account JSON at $serviceAccountPath"
          logger.error(message)
          new RuntimeException(message)
        }
      )
      jsonBytes <- Try(Files.readAllBytes(jsonPath)).toEither
      account <- decode[ServiceAccount](new String(jsonBytes).stripMargin)
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
    logger.info("Getting credentials from provided service-account-json")
    serviceAccount.credentialForScopes(authScopes).toEither
  }
}
