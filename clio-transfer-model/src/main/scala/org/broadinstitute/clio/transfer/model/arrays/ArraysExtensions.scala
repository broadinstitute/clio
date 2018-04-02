package org.broadinstitute.clio.transfer.model.arrays

/**
  *  Extensions for files in the arrays index.
  */
object ArraysExtensions {

  val BpmExtension = ".bpm"

  val CsvExtension = ".csv"

  val DictExtension = ".dict"

  val EgtExtension = ".egt"

  val FastaExtension = ".fasta"

  val FaiExtension = ".fai"

  val GzExtension = ".gz"

  val IdatExtension = ".idat"

  val TbiExtension = ".tbi"

  val ThresholdsExtension = ".thresholds"

  val TxtExtension = ".txt"

  val VcfExtension = ".vcf"

  val GtcExtension = ".gtc"

  val VcfGzExtension = s"${VcfExtension}${GzExtension}"

  val VcfGzTbiExtension = s"${VcfGzExtension}${TbiExtension}"

  val EgtThresholdsTxtExtension = s"${EgtExtension}${ThresholdsExtension}${TxtExtension}"

  val FastaFaiExtension = s"${FastaExtension}${FaiExtension}"
}
