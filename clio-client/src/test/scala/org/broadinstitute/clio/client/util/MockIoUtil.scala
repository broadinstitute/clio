package org.broadinstitute.clio.client.util

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

class MockIoUtil extends IoUtil with LazyLogging {

  private val filesInCloud = mutable.ArrayBuffer.empty[String]

  override def copyGoogleObject(from: String, to: String): Int = {
    if (from.startsWith(MockIoUtil.InvalidPath) || to.startsWith(
          MockIoUtil.InvalidPath
        )) {
      logger.info("Failing on invalid path")
      1
    } else if (!filesInCloud.contains(from)) {
      logger.info(s"Failing on file $from not in mock cloud")
      1
    } else {
      putFileInCloud(to)
      0
    }
  }

  override def deleteGoogleObject(path: String): Int = {
    if (path.startsWith(MockIoUtil.InvalidPath)) {
      logger.info("Failing on invalid path")
      1
    } else
      this.synchronized {
        filesInCloud -= path
        0
      }
  }

  override def googleObjectExists(path: String): Boolean =
    filesInCloud.contains(path)

  def putFileInCloud(path: String) = this.synchronized {
    filesInCloud += path
  }
}

object MockIoUtil {
  val InvalidPath = "gs://not_a_valid_path"
}
