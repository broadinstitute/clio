package org.broadinstitute.clio.integrationtest

import sbt._

/**
  * Scans docker-compose stdout for an exit code.
  *
  * TODO: Remove ExitCodeScanner once docker-compose 1.13 is released on macOS stable, with option --exit-code-from
  * https://github.com/docker/compose/releases/tag/1.13.0
  * https://docs.docker.com/docker-for-mac/
  */
class ExitCodeScanner {
  var loggedExitCodeOption: Option[Int] = None

  def logAndScan(log: Logger)(input: String): Unit = {
    log.info(input)
    input match {
      case ExitCodeScanner.ExitCodeRegex(exitCode) => loggedExitCodeOption = Option(exitCode.toInt)
      case _ => /* ignore */
    }
  }
}

object ExitCodeScanner {
  /** The regex for looking for exit codes. */
  private val ExitCodeRegex =
    """exited with code (-?\d+)""".r.unanchored
}
