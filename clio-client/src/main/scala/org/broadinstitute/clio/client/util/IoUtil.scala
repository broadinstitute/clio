package org.broadinstitute.clio.client.util

import java.io.File
import java.net.URI
import java.nio.file.{FileVisitOption, Files, Path, Paths}
import java.util.Comparator

class IoUtil private[util] (gsUtil: Option[GsUtil]) {

  val googleCloudStorageScheme = "gs"

  val gs: GsUtil = gsUtil.getOrElse(new GsUtil(None))

  def isGoogleObject(location: URI): Boolean =
    Option(location.getScheme).contains(googleCloudStorageScheme)

  def isGoogleDirectory(location: URI): Boolean =
    isGoogleObject(location) && location.getPath.endsWith("/")

  def readMetadata(location: URI): String = {
    if (isGoogleObject(location)) {
      readGoogleObjectData(location)
    } else {
      readFileData(location)
    }
  }

  def readFileData(location: URI): String = {
    new String(Files.readAllBytes(Paths.get(location.getPath)))
  }

  def deleteDirectoryRecursively(directory: Path): Unit = {
    Files
      .walk(directory, FileVisitOption.FOLLOW_LINKS)
      .sorted(Comparator.reverseOrder())
      .map[File](path => path.toFile)
      .forEach(file => file.deleteOnExit())
  }

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
    gs.cat(location.toString)
  }

  def copyGoogleObject(from: URI, to: URI): Int = {
    gs.cp(from.toString, to.toString)
  }

  def deleteGoogleObject(path: URI): Int = {
    gs.rm(path.toString)
  }

  def googleObjectExists(path: URI): Boolean = {
    gs.exists(path.toString) == 0
  }

  private val md5HashPattern = "Hash \\(md5\\):\\s+([0-9a-f]+)".r

  def getMd5HashOfGoogleObject(path: URI): Option[Symbol] = {
    /*
     * Files uploaded through parallel composite uploads won't have an md5 hash.
     * See https://cloud.google.com/storage/docs/gsutil/commands/cp#parallel-composite-uploads
     */
    val rawHash = gs.hash(path.toString)
    md5HashPattern.findFirstMatchIn(rawHash).map(m => Symbol(m.group(1)))
  }

  def getSizeOfGoogleObject(path: URI): Long = {
    gs.du(path.toString)
      .head
      .split("\\s+")
      .head
      .toLong
  }

  def listGoogleObjects(path: URI): Seq[String] = {
    gs.ls(path.toString)
  }
}

object IoUtil extends IoUtil(None)
