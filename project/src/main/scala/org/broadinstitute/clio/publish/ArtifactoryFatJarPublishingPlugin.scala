package org.broadinstitute.clio.publish

import sbt._
import sbt.Keys._

object ArtifactoryFatJarPublishingPlugin extends AutoPlugin {

  override def requires = ArtifactoryPublishingPlugin

  object autoImport {

    /**
      * Ivy configuration used for isolating tasks related to publishing fat jars.
      *
      * All of sbt's built-in ivy logic is catered towards publishing libraries, so if you
      * want to publish stand-alone artifacts you have to jump through hoops to strip out
      * the auto-generated dependency & configuration sections of the ivy XML.
      *
      * Rather than figure out which settings should be overridden in the default `Compile` scope,
      * we make this isolated `FatJar` scope to hold all the ivy settings needed for publishing
      * our fat artifacts.
      */
    lazy val FatJar = config("fat-jar")
  }

  import autoImport._

  private val ivyArtifactPattern =
    "[organization]/[module]/[revision]/[type]s/[artifact](-[classifier])-[revision].[ext]"
  private val ivyXmlPattern =
    "[organization]/[module]/[revision]/[type]s/ivy-[revision].xml"
  private val ivyArtifactoryPatterns = Patterns(
    ivyPatterns = Seq(ivyXmlPattern),
    artifactPatterns = Seq(ivyArtifactPattern),
    isMavenCompatible = true
  )

  /** For ivy-style publishing (only used by the client fat-jar publish, for nightly tests). */
  private val ivyArtifactoryResolver = Resolver.url(
    ArtifactoryPublishingPlugin.ArtifactoryRealm,
    new URL(ArtifactoryPublishingPlugin.ArtifactoryUrl)
  )(ivyArtifactoryPatterns)

  override def projectSettings: Seq[Def.Setting[_]] = inConfig(FatJar) {
    Seq.concat(
      Classpaths.ivyBaseSettings,
      Classpaths.jvmPublishSettings,
      Classpaths.ivyPublishSettings,
      Seq(
        publishTo := Some(ivyArtifactoryResolver),
        publishMavenStyle := false,
        isSnapshot := true,
        moduleSettings := InlineConfigurationWithExcludes(
          projectID.value,
          projectInfo.value,
          Seq.empty,
          configurations = Seq(Default)
        )
      )
    )
  }
}
