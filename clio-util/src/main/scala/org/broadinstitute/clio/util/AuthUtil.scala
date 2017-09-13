package org.broadinstitute.clio.util

import java.nio.file.{Files, Path}

import com.google.auth.oauth2.AccessToken
import io.circe.parser.decode
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import scala.io.Source
import scala.sys.process.Process
import scala.util.Try

object AuthUtil extends ModelAutoDerivation {

  /** Scopes needed from Google to get past Clio's auth proxy. */
  private val authScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  )

  /**
    * Get google credentials either from the service account json or
    * from the user's default setup.
    *
    * @param serviceAccountPath Option of path to the service account json
    */
  def getAccessToken(serviceAccountPath: Option[Path]): AccessToken = {
    serviceAccountPath
      .map { jsonPath =>
        Try(loadServiceAccountJson(jsonPath))
          .map(getCredsFromServiceAccount)
          .getOrElse(
            new AccessToken(
              Process("gcloud auth print-access-token").!!.trim,
              null
            )
          )
      }
      .getOrElse(
        new AccessToken(Process("gcloud auth print-access-token").!!.trim, null)
      )
  }

  def loadServiceAccountJson(serviceAccountPath: Path): ServiceAccount = {
    if (Files.exists(serviceAccountPath)) {
      val jsonBlob =
        Source.fromFile(serviceAccountPath.toFile).mkString.stripMargin
      decode[ServiceAccount](jsonBlob).fold({ error =>
        throw new RuntimeException(
          s"Could not decode service account JSON at $serviceAccountPath",
          error
        )
      }, identity)
    } else {
      throw new RuntimeException(
        s"Could not find service account JSON at $serviceAccountPath"
      )
    }

  }

  def getCredsFromServiceAccount(serviceAccount: ServiceAccount): AccessToken = {
    val creds = serviceAccount.credentialForScopes(authScopes)
    creds.refreshAccessToken()
  }
}
