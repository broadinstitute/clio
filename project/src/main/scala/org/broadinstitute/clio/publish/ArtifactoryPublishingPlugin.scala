package org.broadinstitute.clio.publish

import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

/**
  * A plugin for adding the settings required for clio subprojects to Artifactory.
  *
  * Enable this plugin on every sub-project that should be published.
  */
object ArtifactoryPublishingPlugin extends AutoPlugin {

  override def requires = JvmPlugin

  private def buildTimestamp = System.currentTimeMillis() / 1000

  val ArtifactoryRealm = "Artifactory Realm"

  private val artifactoryHost = "broadinstitute.jfrog.io"

  val ArtifactoryUrl =
    s"https://$artifactoryHost/broadinstitute/libs-release-local;build.timestamp=$buildTimestamp"

  /** For maven-style publishing (the default). */
  private val mavenArtifactoryResolver = ArtifactoryRealm at ArtifactoryUrl

  private val artifactoryUsernameVar = "ARTIFACTORY_USERNAME"
  private val artifactoryPasswordVar = "ARTIFACTORY_PASSWORD"

  private lazy val artifactoryCredentials: Def.Initialize[Option[Credentials]] =
    Def.setting {
      val cred = for {
        username <- sys.env.get(artifactoryUsernameVar)
        password <- sys.env.get(artifactoryPasswordVar)
      } yield {
        Credentials(ArtifactoryRealm, artifactoryHost, username, password)
      }

      cred.orElse {
        // SBT's logging comes from a task, and tasks can't be used inside settings, so we have to roll our own warning...
        println(
          s"[${scala.Console.YELLOW}warn${scala.Console.RESET}] $artifactoryUsernameVar or $artifactoryPasswordVar not set, publishing will fail!"
        )
        None
      }
    }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishTo := Some(mavenArtifactoryResolver)
  )

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    credentials ++= artifactoryCredentials.value.toSeq
  )
}
