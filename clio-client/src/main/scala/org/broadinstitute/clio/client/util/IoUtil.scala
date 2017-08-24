package org.broadinstitute.clio.client.util

import java.io.File
import java.nio.file.{FileVisitOption, Files, Path}
import java.util.Comparator

import scala.concurrent.Future
import scala.io.Source
import scala.sys.process.Process

object IoUtil {

  val googleCloudStorageScheme = "gs://"

  def isGoogleObject(location: String): Boolean =
    location.startsWith(googleCloudStorageScheme)

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

  def moveGoogleObject(from: String, to: String): Future[Unit] = {
    val gs = new GsUtil(None)
    gs.mv(from, to)
  }

  def copyGoogleObject(from: String, to: String): Future[Unit] = {
    val gs = new GsUtil(None)
    gs.cp(from, to)
  }

}

//we should consider moving this to api usage instead of gsutil
class GsUtil(stateDir: Option[Path]) {

  def cp(from: String, to: String): Future[Unit] = {
    runGsUtilAndGetExitCode(Seq("cp", from, to)) match {
      case 0 => Future.successful(())
      case _ => Future.failed(new Exception(s"The gsutil copy command from $from to $to failed"))
    }
  }

  def mv(from: String, to: String): Future[Unit] = {
    runGsUtilAndGetExitCode(Seq("mv", from, to)) match {
      case 0 => Future.successful(())
      case _ => Future.failed(new Exception(s"The gsutil move command from $from to $to failed"))
    }
  }

  def cat(objectLocation: String): String = {
    runGsUtilAndGetStdout(Seq("cat", objectLocation))
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
