package org.broadinstitute.clio.client.util

import java.net.URI

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

class MockIoUtil extends IoUtil with LazyLogging {

  private val filesInCloud = mutable.ArrayBuffer.empty[URI]

  override def copyGoogleObject(from: URI, to: URI): Int = {
    if (MockIoUtil.isInvalid(from) || MockIoUtil.isInvalid(to)) {
      logger.info("Failing on invalid path")
      1
    } else if (!filesInCloud.contains(from)) {
      logger.info(s"Failing on file $from not in mock cloud")
      1
    } else {
      val fromString = from.toString
      if (to.toString.endsWith("/")) {
        putFileInCloud(
          URI.create(
            to.toString + fromString
              .substring(fromString.lastIndexOf('/') + 1, fromString.length)
          )
        )
      } else {
        putFileInCloud(to)
      }
      0
    }
  }

  override def deleteGoogleObject(path: URI): Int = {
    if (MockIoUtil.isInvalid(path)) {
      logger.info("Failing on invalid path")
      1
    } else
      this.synchronized {
        filesInCloud -= path
        0
      }
  }

  override def googleObjectExists(path: URI): Boolean =
    filesInCloud.contains(path)

  def putFileInCloud(path: URI): Unit = this.synchronized {
    val _ = filesInCloud += path
  }
}

object MockIoUtil {
  val InvalidPath: URI = URI.create("/not_a_valid_path")

  private def isInvalid(path: URI): Boolean =
    path.getPath.startsWith(InvalidPath.getPath)
}
