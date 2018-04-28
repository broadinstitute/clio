package org.broadinstitute.clio.util.auth

import java.util.Collections

import better.files.File
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}

import scala.collection.JavaConverters._

class ClioCredentials(baseCredentials: GoogleCredentials) {
  import ClioCredentials._

  def this(serviceAccountOverride: Option[File]) = this {
    serviceAccountOverride.fold(GoogleCredentials.getApplicationDefault)(
      _.inputStream.apply(ServiceAccountCredentials.fromStream)
    )
  }

  def oauth(): GoogleCredentials = baseCredentials.createScoped(oauthScopes)

  def storage(readOnly: Boolean): GoogleCredentials = {
    val scope = if (readOnly) readOnlyStorageScope else readWriteStorageScope
    baseCredentials.createScoped(Collections.singleton(scope))
  }
}

object ClioCredentials {

  /** Scopes needed from Google to get past Clio's auth proxy. */
  private[clio] val oauthScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  ).asJava

  /** Scope needed to allow reads from, and prevent writes to, cloud storage. */
  private[clio] val readOnlyStorageScope =
    "https://www.googleapis.com/auth/devstorage.read_only"

  /** Scope needed to allow reads from amd writes to cloud storage. */
  private[clio] val readWriteStorageScope =
    "https://www.googleapis.com/auth/devstorage.read_write"
}
