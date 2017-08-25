package org.broadinstitute.client.util

import org.broadinstitute.clio.client.util.IoUtil

object MockIoUtil extends IoUtil {

  var filesInCloud: Seq[String] = Seq()

  override def copyGoogleObject(from: String, to: String): Int = {
    if (from equals to) return 1
    if (!filesInCloud.contains(from)) return 1
    putFileInCloud(to)
    0
  }

  override def deleteGoogleObject(path: String): Int = {
    filesInCloud = filesInCloud.filterNot(_ equals path)
    0
  }

  override def googleObjectExists(path: String): Boolean =
    filesInCloud.contains(path)

  def putFileInCloud(path: String) = filesInCloud = filesInCloud :+ path

  def deleteAllInCloud() = filesInCloud = Seq()
}
