package org.broadinstitute.clio.util.auth

import java.util.Collections

import better.files.File
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}

import scala.collection.JavaConverters._

/** Wrapper for Google credentials, providing helpers for setting up authorization scopes. */
class ClioCredentials(baseCredentials: GoogleCredentials) {
  import ClioCredentials._

  def this(serviceAccountOverride: Option[File]) = this {
    serviceAccountOverride.fold(GoogleCredentials.getApplicationDefault)(
      _.inputStream.apply(ServiceAccountCredentials.fromStream)
    )
  }

  /** Build credentials which can provide user info. */
  def userInfo(): GoogleCredentials = baseCredentials.createScoped(userInfoScopes)

  /** Build credentials which can perform cloud I/O. */
  def storage(readOnly: Boolean): GoogleCredentials = {
    val scope = if (readOnly) readOnlyStorageScope else readWriteStorageScope
    baseCredentials.createScoped(Collections.singleton(scope))
  }
}

object ClioCredentials {

  /** Scopes needed to get user info from a Google account to prove "valid user" identity. */
  private[auth] val userInfoScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  ).asJava

  /** Scope needed to allow reads from, and prevent writes to, cloud storage. */
  private[auth] val readOnlyStorageScope =
    "https://www.googleapis.com/auth/devstorage.read_only"

  /** Scope needed to allow reads from amd writes to cloud storage. */
  private[auth] val readWriteStorageScope =
    "https://www.googleapis.com/auth/devstorage.read_write"
}
