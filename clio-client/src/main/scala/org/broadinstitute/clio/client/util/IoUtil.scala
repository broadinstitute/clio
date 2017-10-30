package org.broadinstitute.clio.client.util

import java.io.File
import java.nio.file.{FileVisitOption, Files, Path}
import java.util.Comparator

import scala.io.Source
import scala.sys.process.Process

trait IoUtil {
  val googleCloudStorageScheme = "gs://"

  def isGoogleObject(location: String): Boolean =
    location.startsWith(googleCloudStorageScheme)

  def isGoogleDirectory(location: String): Boolean =
    isGoogleObject(location) && location.endsWith("/")

  def readMetadata(location: String): String = {
    if (isGoogleObject(location)) {
      readGoogleObjectData(location)
    } else {
      readFileData(location)
    }
  }

  def readFileData(location: String): String = {
    Source.fromFile(new File(location)).getLines().mkString
  }

  def readGoogleObjectData(location: String): String = {
    val gs = new GsUtil(None)
    gs.cat(location)
  }

  def deleteDirectoryRecursively(directory: Path): Unit = {
    Files
      .walk(directory, FileVisitOption.FOLLOW_LINKS)
      .sorted(Comparator.reverseOrder())
      .map[File](path => path.toFile)
      .forEach(file => file.deleteOnExit())
  }

  def copyGoogleObject(from: String, to: String): Int = {
    val gs = new GsUtil(None)
    gs.cp(from, to)
  }

  def deleteGoogleObject(path: String): Int = {
    val gs = new GsUtil(None)
    gs.rm(path)
  }

  def googleObjectExists(path: String): Boolean = {
    val gs = new GsUtil(None)
    gs.exists(path) == 0
  }

  def getMd5HashOfGoogleObject(path: String): String = {
    new GsUtil(None).md5Hash(path)
  }

  def getSizeOfGoogleObject(path: String): Long = {
    new GsUtil(None).du(path).head.split(" +").head.toLong
  }

  def listGoogleObjects(path: String): Seq[String] = {
    new GsUtil(None).ls(path)
  }

}
object IoUtil extends IoUtil {}

//we should consider moving this to api usage instead of gsutil
class GsUtil(stateDir: Option[Path]) {

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

  def md5Hash(path: String): String = {
    val output = runGsUtilAndGetStdout(Seq("hash", "-m", "-h", path))
    output.lines.filter(_.contains("md5")).next.split(":")(1).trim
  }

  def runGsUtilAndGetStdout(gsUtilArgs: Seq[String]): String = {
    val str = getGsUtilCmdWithStateDir(gsUtilArgs).mkString(" ")
    Process(str).!!
  }

  def runGsUtilAndGetExitCode(gsUtilArgs: Seq[String]): Int = {
    val str = getGsUtilCmdWithStateDir(gsUtilArgs).mkString(" ")
    Process(str).!
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
