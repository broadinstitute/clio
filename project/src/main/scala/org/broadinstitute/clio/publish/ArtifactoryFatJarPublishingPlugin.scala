package org.broadinstitute.clio.publish

import sbt._
import sbt.Keys._

object ArtifactoryFatJarPublishingPlugin extends AutoPlugin {

  override def requires = ArtifactoryPublishingPlugin

  object autoImport {

    /**
      * Ivy configuration used for isolating tasks related to publishing.
      *
      * All of sbt's built-in ivy logic is catered towards publishing libraries, so if you
      * want to publish stand-alone artifacts you have to jump through hoops to strip out
      * the auto-generated dependency & configuration sections of the ivy XML.
      *
      * Rather than figure out which settings should be overridden in the default `Compile` scope,
      * we make this isolated `Publish` scope to hold all the ivy settings needed for publishing
      * our fat artifacts.
      */
    lazy val Publish = config("publish")
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

  /** Initialization settings for publishing via ivy, scoped to the new `Publish` configuration. */
  private lazy val basePublishSettings = inConfig(Publish) {
    Seq.concat(
      Classpaths.ivyBaseSettings,
      Classpaths.jvmPublishSettings,
      Classpaths.ivyPublishSettings,
      Seq(
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

  override def projectSettings: Seq[Def.Setting[_]] = Seq.concat(
    basePublishSettings,
    Seq(
      publishTo := Some(ivyArtifactoryResolver),
      publishMavenStyle := false,
      /*
       * Disable publishing of library-style artifacts.
       * Individual sub-projects must re-enable publishing of
       * whatever "fat" artifact(s) they produce.
       */
      publishArtifact in (Compile, packageBin) := false,
      publishArtifact in (Compile, packageDoc) := false,
      publishArtifact in (Compile, packageSrc) := false,
      /*
       * Redirect the top-level ivy-delivery and publishing tasks to
       * point at the same tasks scoped to the `Publish` configuration,
       * to avoid accidentally publishing library-style artifacts.
       */
      deliverLocal := (deliverLocal in Publish).value,
      deliver := (deliver in Publish).value,
      publishLocal := (publishLocal in Publish).value,
      publish := (publish in Publish).value
    )
  )
}
