package org.broadinstitute.clio.sbt

import com.typesafe.sbt.SbtGit._
import sbt.Def.Initialize
import sbt.Keys._
import sbt._

/**
  * Versioning information for builds of clio, based on a dotted
  * version appended with a git hash.
  */
object Versioning {

  /**
    * Read the base version from file. This version number
    * if this is a release commit, or the upcoming version number
    * if this is a snapshot.
    */
  lazy val clioBaseVersion: Initialize[String] = Def.setting {
    IO.read(baseDirectory(_ / ".clio-version").value).stripLineEnd
  }

  /** Generate a version using the first 7 chars of the git hash, or "UNKNOWN". */
  lazy val gitShaVersion: Initialize[Option[String]] = Def.setting {
    val base = git.baseVersion.value
    val hash = git.gitHeadCommit.value match {
      case Some(sha) => s"g${sha.take(7)}"
      case None      => "UNKNOWN"
    }
    Option(s"$base-$hash-SNAP")
  }

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
