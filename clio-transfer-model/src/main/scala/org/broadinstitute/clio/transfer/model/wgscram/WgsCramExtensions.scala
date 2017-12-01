package org.broadinstitute.clio.transfer.model.wgscram

/** Expected extensions for files tracked by the wgs-cram index. */
object WgsCramExtensions {

  /** File extension for crams. */
  val CramExtension = ".cram"

  /** Extra extension for cram indices. */
  val CraiExtensionAddition = ".crai"

  /** Full extension for cram indices. */
  val CraiExtension = s"$CramExtension$CraiExtensionAddition"

  /** Extra extension for cram md5s. */
  val Md5ExtensionAddition = ".md5"

  /** Full extension for cram md5s. */
  val Md5Extension = s"$CramExtension$Md5ExtensionAddition"
}
