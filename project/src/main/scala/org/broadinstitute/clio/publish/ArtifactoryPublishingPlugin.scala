package org.broadinstitute.clio.publish

import sbt._
import sbt.Keys._

/**
  * A plugin for adding the settings required for clio subprojects to Artifactory.
  *
  * Enable this plugin on every sub-project that should be published.
  */
object ArtifactoryPublishingPlugin extends AutoPlugin {

  private def buildTimestamp = System.currentTimeMillis() / 1000

  private val artifactoryHost = "broadinstitute.jfrog.io"
  private val artifactoryRealm = "Artifactory Realm"
  private val artifactoryResolver =
    artifactoryRealm at s"https://$artifactoryHost/libs-release-local;build.timestamp=$buildTimestamp"

  private val artifactoryUsernameVar = "ARTIFACTORY_USERNAME"
  private val artifactoryPasswordVar = "ARTIFACTORY_PASSWORD"

  private lazy val artifactoryCredentials
    : Def.Initialize[Task[Option[Credentials]]] =
    Def.task {
      val log = streams.value.log

      val cred = for {
        username <- sys.env.get(artifactoryUsernameVar)
        password <- sys.env.get(artifactoryPasswordVar)
      } yield {
        Credentials(artifactoryRealm, artifactoryHost, username, password)
      }

      cred.orElse {
        log.warn(
          s"$artifactoryUsernameVar or $artifactoryPasswordVar not set, publishing will fail!"
        )
        None
      }
    }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishTo := Some(artifactoryResolver)
  )

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    credentials ++= artifactoryCredentials.value.toSeq
  )
}
