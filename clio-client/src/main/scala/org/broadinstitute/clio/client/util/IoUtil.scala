package org.broadinstitute.clio.client.util

import java.net.URI

import better.files.File
import com.google.cloud.storage.{BlobId, StorageOptions}

/**
  * Clio-client component handling all file IO operations.
  */
trait IoUtil {

  import IoUtil._

  protected def gsUtil: GsUtil

  def isGoogleObject(location: URI): Boolean =
    Option(location.getScheme).contains(GoogleCloudStorageScheme)

  def isGoogleDirectory(location: URI): Boolean =
    isGoogleObject(location) && location.getPath.endsWith("/")

  def readMetadata(location: URI): String = {
    if (isGoogleObject(location)) {
      readGoogleObjectData(location)
    } else {
      readFileData(location)
    }
  }

  def readFileData(location: URI): String =
    File(location.getPath).contentAsString

  /*
   * FIXME for all below:
   *
   * Add a "credentials" argument (explicit or implicit) and
   * use it to authenticate to GCS so we can convert the URIs
   * into Paths and use the NIO API.
   *
   * We can't just parse as Paths from the start because the
   * google-cloud-nio adapter will eagerly check storage authentication
   * for each path, and fail.
   */

  def readGoogleObjectData(location: URI): String = {
    gsUtil.cat(location.toString)
  }

  def copyGoogleObject(from: URI, to: URI): Unit = {
    gsUtil.cp(from.toString, to.toString)
  }

  def deleteGoogleObject(path: URI): Unit = {
    gsUtil.rm(path.toString)
  }

  def googleObjectExists(path: URI): Boolean = {
    gsUtil.exists(path.toString)
  }
}

object IoUtil extends IoUtil {
  private val GoogleCloudStorageScheme = "gs"
  private val GoogleCloudPathPrefix = GoogleCloudStorageScheme + "//"
  private val GoogleCloudPathSeparator = "/"

  override val gsUtil: GsUtil = new GsUtil

  class GsUtil {

    private val storage = StorageOptions.getDefaultInstance.getService

    private def toBlobId(path: String) = {
      val noPrefix = path.substring(GoogleCloudPathPrefix.length)
      val firstSeparator = noPrefix.indexOf(GoogleCloudPathSeparator)
      // Get the bucket and the object name (aka path) from the gcs path.
      BlobId.of(
        noPrefix.substring(0, firstSeparator),
        noPrefix.substring(firstSeparator + 1)
      )
    }

    private def getBlob(path: String) = {
      storage.get(toBlobId(path))
    }

    def cp(from: String, to: String): Unit = {
      getBlob(from).copyTo(toBlobId(to)).getResult.asInstanceOf[Unit]
    }

    def rm(path: String): Unit = {
      if (!getBlob(path).delete()) {
        throw new RuntimeException(s"Cannot delete '$path' because it does not exist.")
      }
    }

    def cat(objectLocation: String): String = {
      new String(getBlob(objectLocation).getContent())
    }

    def exists(path: String): Boolean = {
      getBlob(path).exists()
    }
  }
}
