package org.broadinstitute.clio.sbt

import com.typesafe.sbt.SbtGit._
import sbt.Def.Initialize
import sbt.Keys._
import sbt._

/** Versioning information for builds of clio, based on the git hash. */
object Versioning {

  /** Generate a version using the full git hash, or "UNKNOWN". */
  lazy val gitShaVersion: Initialize[String] =
    Def.setting(git.gitHeadCommit.value.getOrElse("UNKNOWN"))

  /** Customize the executable jar name with just the version. */
  lazy val assemblyName: Initialize[String] = Def.setting {
    s"${name.value}-${version.value}.jar"
  }

  /** Write the version information into a configuration file. */
  lazy val writeVersionConfig: Initialize[Task[Seq[File]]] = Def.task {
    val projectName = name.value
    val projectConfig = projectName.replace("clio-", "clio.")
    val file = (resourceManaged in Compile).value / s"$projectName-version.conf"
    val contents = Seq(s"$projectConfig.version: ${version.value}")
    IO.writeLines(file, contents)
    Seq(file)
  }
}
