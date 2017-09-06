package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.ClioServerConfig.Persistence

import com.google.cloud.storage.contrib.nio.{
  CloudStorageConfiguration,
  CloudStorageFileSystem
}
import com.google.cloud.storage.StorageOptions

import java.nio.file.{FileSystem, Path}

/**
  * Persistence DAO which writes to GCS.
  * Uses Google's cloud-storage NIO FileSystem provider to
  * abstract over the fact that we're writing to the cloud.
  */
class GcsPersistenceDAO(gcsConfig: Persistence.GcsConfig)
    extends PersistenceDAO {

  private lazy val storageOptions = StorageOptions
    .newBuilder()
    .setProjectId(gcsConfig.projectId)
    .setCredentials(
      gcsConfig.account.credentialForScopes(GcsPersistenceDAO.storageScopes)
    )
    .build()

  private lazy val gcs: FileSystem =
    CloudStorageFileSystem.forBucket(
      gcsConfig.bucket,
      CloudStorageConfiguration.DEFAULT,
      storageOptions
    )

  override lazy val rootPath: Path = gcs.getPath("/")
}

object GcsPersistenceDAO {

  /**
    * Authorization scopes required for writing to Clio's GCS buckets.
    */
  val storageScopes: Seq[String] = Seq(
    "https://www.googleapis.com/auth/devstorage.read_write"
  )
}
