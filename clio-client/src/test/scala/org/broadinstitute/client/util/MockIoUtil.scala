package org.broadinstitute.client.util

import org.broadinstitute.clio.client.util.IoUtil

class MockIoUtil extends IoUtil {

  var filesInCloud: Seq[String] = Seq()

  override def copyGoogleObject(from: String, to: String): Int = {
    if (from.startsWith("gs://not_a_valid_path") ||
        to.startsWith("gs://not_a_valid_path")) return 1
    if (from equals to) return 1
    if (!filesInCloud.contains(from)) return 1
    putFileInCloud(to)
    0
  }

  override def deleteGoogleObject(path: String): Int = {
    if (path.startsWith("error://")) return 1
    filesInCloud = filesInCloud.filterNot(_ equals path)
    0
  }

  override def googleObjectExists(path: String): Boolean =
    filesInCloud.contains(path)

  def putFileInCloud(path: String) = filesInCloud = filesInCloud :+ path

  def resetMockState() = filesInCloud = Seq()
}
