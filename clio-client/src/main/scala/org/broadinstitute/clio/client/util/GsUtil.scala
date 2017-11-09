package org.broadinstitute.clio.client.util

import java.nio.file.{Files, Path}

import scala.sys.process.Process

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
