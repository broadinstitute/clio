package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.ClioServerConfig.Persistence

import com.google.cloud.storage.contrib.nio.{
  CloudStorageConfiguration,
  CloudStorageFileSystem
}
import com.google.cloud.storage.StorageOptions

import java.nio.file.{FileSystem, Files, NoSuchFileException, Path}

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

  override def checkRoot(): Unit = {
    val versionPath = rootPath.resolve(GcsPersistenceDAO.versionFileName)
    try {
      val versionFile =
        Files.write(versionPath, ClioServerConfig.Version.value.getBytes)
      logger.debug(s"Wrote current Clio version to ${versionFile.toUri}")
    } catch {
      case e: NoSuchFileException => {
        throw new RuntimeException(
          s"Couldn't write Clio version to GCS at $versionPath, aborting!",
          e
        )
      }
    }

  }
}

object GcsPersistenceDAO {

  /**
    * Authorization scopes required for writing to Clio's GCS buckets.
    */
  val storageScopes: Seq[String] = Seq(
    "https://www.googleapis.com/auth/devstorage.read_write"
  )

  /**
    * Name of the dummy file that Clio will write to the root of its configured
    * GCS bucket at startup, to ensure the given config is good.
    *
    * We have to write a dummy file because the google-cloud-nio adapter assumes
    * that all directories exist and are writeable by design, since directories
    * aren't really a thing in GCS.
    */
  val versionFileName = "current-clio-version.txt"
}
