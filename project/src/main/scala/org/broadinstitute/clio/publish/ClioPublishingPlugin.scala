package org.broadinstitute.clio.publish

import sbt._
import sbt.Keys._

/**
  * A plugin for adding the settings required for clio subprojects to Artifactory.
  *
  * Enable this plugin on every sub-project that should be published.
  */
object ClioPublishingPlugin extends AutoPlugin {

  private def buildTimestamp = System.currentTimeMillis() / 1000

  private val artifactoryHost = "broadinstitute.jfrog.io"

  private def artifactoryResolver(isSnapshot: Boolean): Resolver = {
    val repoType = if (isSnapshot) "snapshot" else "release"
    val repoUrl =
      s"https://$artifactoryHost/broadinstitute/libs-$repoType-local;build.timestamp=$buildTimestamp"
    val repoName = "artifactory-publish"
    repoName at repoUrl
  }

  private val artifactoryCredentials: Credentials = {
    val username = sys.env.getOrElse("ARTIFACTORY_USERNAME", "")
    val password = sys.env.getOrElse("ARTIFACTORY_PASSWORD", "")
    Credentials("Artifactory Realm", artifactoryHost, username, password)
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishTo := Some(artifactoryResolver(isSnapshot.value)),
    credentials += artifactoryCredentials
  )
}
