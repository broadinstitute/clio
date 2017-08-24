package org.broadinstitute.clio.util.model

import com.google.auth.oauth2.ServiceAccountCredentials

import scala.collection.JavaConverters._

import java.net.URI

/**
  * Representation of service-account JSON produced by GCloud,
  * used for persisting Clio updates to cloud storage / for
  * communicating through Clio's OpenIDC proxy.
  */
case class ServiceAccount(authProviderX509CertUrl: URI,
                          authUri: URI,
                          clientEmail: String,
                          clientId: String,
                          clientX509CertUrl: URI,
                          privateKey: String,
                          privateKeyId: String,
                          projectId: String,
                          tokenUri: URI,
                          `type`: String) {

  assert(`type` == "service_account")

  def credentialForScopes(scopes: Seq[String]): ServiceAccountCredentials = {
    ServiceAccountCredentials.fromPkcs8(
      clientId,
      clientEmail,
      privateKey,
      privateKeyId,
      scopes.asJava,
      null,
      tokenUri
    )
  }
}
