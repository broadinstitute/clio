package org.broadinstitute.clio.transfer.model.bam

import java.net.URI

object BamExtensions {

  /** File extension for bams. */
  val BamExtension = ".bam"

  /** Extra extension for bam indices. */
  val BaiExtension = ".bai"

  /** Extra extension for bam md5s. */
  val Md5ExtensionAddition = ".md5"

  /** Full extension for bam md5s. */
  val Md5Extension = s"$BamExtension$Md5ExtensionAddition"

  def replaceBamExtensionWithBaiExtension(bamUri: URI): URI = {
    URI.create(bamUri.toString.replaceAll(BamExtension + "$", BaiExtension))
  }

}
