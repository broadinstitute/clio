package org.broadinstitute.clio.client.util

import java.io.File
import java.net.URI
import java.nio.file.{FileVisitOption, Files, Path, Paths}
import java.util.Comparator

import scala.sys.process.Process

class IoUtil private[util] (gsUtil: Option[IoUtil.GsUtil]) {

  val googleCloudStorageScheme = "gs"

  val gs: IoUtil.GsUtil = gsUtil.getOrElse(new IoUtil.GsUtil(None))

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

object IoUtil extends IoUtil(None) {

  /**
    * Wrapper around gsutil managing state directory creation
    * and basic result parsing.
    *
    * TODO: We should consider moving this to google-cloud-java
    * api usage instead of gsutil. We pay significant overhead
    * on the startup time of every gsutil call.
    */
  private[util] class GsUtil(stateDir: Option[Path]) {

    def ls(path: String): Seq[String] = {
      runGsUtilAndGetStdout(Seq("ls", path)).split("\n")
    }

    def du(path: String): Seq[String] = {
      runGsUtilAndGetStdout(Seq("du", path)).split("\n")
    }

    def cp(from: String, to: String): Int = {
      runGsUtilAndGetExitCode(Seq("cp", from, to))
    }

    def mv(from: String, to: String): Int = {
      runGsUtilAndGetExitCode(Seq("mv", from, to))
    }

    def rm(path: String): Int = {
      runGsUtilAndGetExitCode(Seq("rm", path))
    }

    def cat(objectLocation: String): String = {
      runGsUtilAndGetStdout(Seq("cat", objectLocation))
    }

    def exists(path: String): Int = {
      runGsUtilAndGetExitCode(Seq("-q", "stat", path))
    }

    def hash(path: String): String = {
      runGsUtilAndGetStdout(Seq("hash", "-m", "-h", path))
    }

    def runGsUtilAndGetStdout(gsUtilArgs: Seq[String]): String = {
      val cmd = getGsUtilCmdWithStateDir(gsUtilArgs)
      Process(cmd).!!
    }

    def runGsUtilAndGetExitCode(gsUtilArgs: Seq[String]): Int = {
      val cmd = getGsUtilCmdWithStateDir(gsUtilArgs)
      Process(cmd).!
    }

    def getGsUtilCmdWithStateDir(gsUtilArgs: Seq[String]): Seq[String] = {
      def getTempStateDir: Path = {
        val tempStateDir = Files.createTempDirectory("gsutil-save")
        sys.addShutdownHook({
          IoUtil.deleteDirectoryRecursively(tempStateDir)
        })
        tempStateDir
      }

      Seq(
        "gsutil",
        "-o",
        "GSUtil:state_dir=" + stateDir.getOrElse(getTempStateDir)
      ) ++ gsUtilArgs
    }
  }
}
