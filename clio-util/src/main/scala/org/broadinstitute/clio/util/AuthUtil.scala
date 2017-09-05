package org.broadinstitute.clio.util

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.parser.decode
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import scala.io.Source
import scala.sys.process.Process

object AuthUtil extends ModelAutoDerivation with ErrorAccumulatingCirceSupport {

  /** Scopes needed from Google to get past Clio's auth proxy. */
  private val authScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  )

  def getBearerToken(serviceAccountPath: Option[String]): OAuth2BearerToken = {
    serviceAccountPath
      .map { jsonPath =>
        Source.fromFile(jsonPath).getLines.mkString.stripMargin
      }
      .map { jsonBlob =>
        decode[ServiceAccount](jsonBlob).fold({ error =>
          throw new RuntimeException(error)
        }, identity)
      }
      .map(getBearerTokenFromServiceAccount)
      .orElse {
        Option(OAuth2BearerToken(Process("gcloud auth print-access-token").!!.trim))
      }
      .getOrElse(throw new RuntimeException("Could not get bearer token"))
  }

  def getBearerTokenFromServiceAccount(
    serviceAccount: ServiceAccount
  ): OAuth2BearerToken = {
    val creds = serviceAccount.credentialForScopes(authScopes)
    OAuth2BearerToken(creds.refreshAccessToken().getTokenValue)

  }
}
