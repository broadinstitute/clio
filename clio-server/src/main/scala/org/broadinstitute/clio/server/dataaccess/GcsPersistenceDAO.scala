package org.broadinstitute.clio.server.dataaccess

import java.nio.file.FileSystem

import better.files.File
import com.google.cloud.storage.StorageOptions
import com.google.cloud.storage.contrib.nio.{
  CloudStorageConfiguration,
  CloudStorageFileSystem
}
import org.broadinstitute.clio.server.ClioServerConfig.Persistence

/**
  * Persistence DAO which writes to GCS.
  * Uses Google's cloud-storage NIO FileSystem provider to
  * abstract over the fact that we're writing to the cloud.
  */
class GcsPersistenceDAO(gcsConfig: Persistence.GcsConfig, recoveryParallelism: Int)
    extends PersistenceDAO(recoveryParallelism) {

  private lazy val storageOptions = {
    StorageOptions
      .newBuilder()
      .setProjectId(gcsConfig.projectId)
      .setCredentials(gcsConfig.creds.storage(readOnly = false))
      .build()
  }

  private lazy val gcs: FileSystem =
    CloudStorageFileSystem.forBucket(
      gcsConfig.bucket,
      CloudStorageConfiguration.DEFAULT,
      storageOptions
    )

  override lazy val rootPath: File = File(gcs.getPath("/"))
}
