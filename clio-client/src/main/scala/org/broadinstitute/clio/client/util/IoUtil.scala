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

  def copyGoogleObject(from: URI, to: URI): Int = {
    gsUtil.cp(from.toString, to.toString)
  }

  def deleteGoogleObject(path: URI): Int = {
    gsUtil.rm(path.toString)
  }

  def googleObjectExists(path: URI): Boolean = {
    gsUtil.exists(path.toString) == 0
  }

  def getSizeOfGoogleObject(path: URI): Long = {
    gsUtil.du(path.toString)
  }
}

object IoUtil extends IoUtil {
  val GoogleCloudStorageScheme = "gs"

  override val gsUtil: GsUtil = new GsUtil

  /**
    * Wrapper around gsutil managing state directory creation
    * and basic result parsing.
    *
    * TODO: We should consider moving this to google-cloud-java
    * api usage instead of gsutil. We pay significant overhead
    * on the startup time of every gsutil call.
    */
  class GsUtil {

    private val storage = StorageOptions.getDefaultInstance.getService

    private def toBlobId(path: String) = {
      val noprefix = path.substring("gs://".length)
      val firstSlash = noprefix.indexOf("/")
      BlobId.of(noprefix.substring(0, firstSlash), noprefix.substring(firstSlash + 1))
    }

    private def getBlob(path: String) = {
      storage.get(toBlobId(path))
    }

    def du(path: String): Long = {
      getBlob(path).getSize
    }

    def cp(from: String, to: String): Int = {
      getBlob(from).copyTo(toBlobId(to)).getResult
      1
    }

    def mv(from: String, to: String): Int = {
      cp(from, to)
      rm(from)
    }

    def rm(path: String): Int = {
      getBlob(path).delete()
      1
    }

    def cat(objectLocation: String): String = {
      new String(getBlob(objectLocation).getContent())
    }

    def exists(path: String): Int = {
      if (getBlob(path).exists()) 1 else 0
    }

    def hash(path: String): String = {
      getBlob(path).getMd5
    }
  }
}
