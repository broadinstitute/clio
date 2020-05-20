package org.broadinstitute.clio.transfer.model.bam

object BamExtensions {

  /** File extension for bams. */
  val BamExtension = ".bam"

  /** Extra extension for bam indices. */
  val BaiExtensionAddition = ".bai"

  /** Full extension for bam indices. */
  val BaiExtension = s"$BamExtension$BaiExtensionAddition"

  /** Extra extension for bam md5s. */
  val Md5ExtensionAddition = ".md5"

  /** Full extension for bam md5s. */
  val Md5Extension = s"$BamExtension$Md5ExtensionAddition"

}
