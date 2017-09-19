package org.broadinstitute.clio.util

import java.nio.file.{Files, Path}
import com.google.auth.oauth2.AccessToken
import io.circe.parser.decode
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import com.typesafe.scalalogging.LazyLogging

import scala.sys.process.Process
import scala.util.Try

object AuthUtil extends ModelAutoDerivation with LazyLogging {

  /** Scopes needed from Google to get past Clio's auth proxy. */
  private val authScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  )

  /** Get an access token by calling out to gcloud. */
  private def shellOutAuthToken(): Either[Throwable, AccessToken] =
    Try {
      new AccessToken(Process("gcloud auth print-access-token").!!.trim, null)
    }.toEither

  /**
    * Get google credentials either from the service account json or
    * from the user's default setup.
    *
    * @param serviceAccountPath Option of path to the service account json
    */
  def getAccessToken(
    serviceAccountPath: Option[Path]
  ): Either[Throwable, AccessToken] = {
    serviceAccountPath.fold(shellOutAuthToken()) { jsonPath =>
      loadServiceAccountJson(jsonPath)
        .fold(err => {
          logger.warn(
            s"Failed to load service account JSON at $jsonPath, falling back to gcloud",
            err
          )
          shellOutAuthToken()
        }, getCredsFromServiceAccount)
    }
  }

  /** Load JSON for a google service account. */
  def loadServiceAccountJson(
    serviceAccountPath: Path
  ): Either[Throwable, ServiceAccount] = {
    for {
      jsonPath <- Either.cond(
        Files.exists(serviceAccountPath),
        serviceAccountPath,
        new RuntimeException(
          s"Could not find service account JSON at $serviceAccountPath"
        )
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
  ): Either[Throwable, AccessToken] = {
    val tokenOrErr = for {
      creds <- serviceAccount.credentialForScopes(authScopes)
      token <- Try(creds.refreshAccessToken())
    } yield {
      token
    }

    tokenOrErr.toEither
  }
}
