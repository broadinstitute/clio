package org.broadinstitute.clio.util.model

import com.google.auth.oauth2.ServiceAccountCredentials

import scala.collection.JavaConverters._

import java.net.URI

case class ServiceAccount(clientId: String,
                          clientEmail: String,
                          privateKey: String,
                          privateKeyId: String,
                          tokenUri: URI) {

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
