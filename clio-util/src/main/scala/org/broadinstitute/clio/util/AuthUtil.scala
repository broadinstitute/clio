package org.broadinstitute.clio.util

import java.nio.file.Path

import com.google.auth.oauth2.AccessToken
import io.circe.parser.decode
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import scala.io.Source
import scala.sys.process.Process

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
        loadServiceAccountJson(jsonPath)
      }.fold(new AccessToken(
      Process("gcloud auth print-access-token").!!.trim,
      null
    ))(getCredsFromServiceAccount)
  }

  def loadServiceAccountJson(serviceAccountPath: Path): ServiceAccount = {
    val jsonBlob =
      Source.fromFile(serviceAccountPath.toFile).mkString.stripMargin
    decode[ServiceAccount](jsonBlob).fold({ error =>
      throw new RuntimeException(
        s"Could not decode service account JSON at $serviceAccountPath",
        error
      )
    }, identity)
  }

  def getCredsFromServiceAccount(serviceAccount: ServiceAccount): AccessToken = {
    val creds = serviceAccount.credentialForScopes(authScopes)
    creds.refreshAccessToken()
  }
}
