package org.broadinstitute.clio.util

import java.nio.file.{Files, Path}
import com.google.auth.oauth2.AccessToken
import io.circe.parser.decode
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import scala.sys.process.Process
import scala.util.Try

object AuthUtil extends ModelAutoDerivation {

  /** Scopes needed from Google to get past Clio's auth proxy. */
  private val authScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  )

  /** Get an access token by calling out to gcloud. */
  private def shellOutAuthToken: Either[Throwable, AccessToken] =
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
    serviceAccountPath
      .map { jsonPath =>
        loadServiceAccountJson(jsonPath)
          .flatMap(getCredsFromServiceAccount)
          .left
          .flatMap(_ => shellOutAuthToken)
      }
      .getOrElse(shellOutAuthToken)
  }

  /** Load JSON for a google service account. */
  def loadServiceAccountJson(
    serviceAccountPath: Path
  ): Either[Throwable, ServiceAccount] = {
    val accountOrErr = for {
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

    accountOrErr.left.map { error =>
      new RuntimeException(
        s"Could not decode service account JSON at $serviceAccountPath",
        error
      )
    }
  }

  /**
    * Request an access token for the given service account with the
    * scopes needed to talk through Clio's OpenIDC proxy.
    */
  def getCredsFromServiceAccount(
    serviceAccount: ServiceAccount
  ): Either[Throwable, AccessToken] =
    Try {
      val creds = serviceAccount.credentialForScopes(authScopes)
      creds.refreshAccessToken()
    }.toEither
}
