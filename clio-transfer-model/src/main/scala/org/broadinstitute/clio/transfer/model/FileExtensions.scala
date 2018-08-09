package org.broadinstitute.clio.transfer.model

trait FileExtensions {
  val TxtExtension = ".txt"

  val CsvExtension = ".csv"

  val FastaExtension = ".fasta"

  val FaiExtension = ".fai"

  val FastaFaiExtension = s"$FastaExtension$FaiExtension"

  val DictExtension = ".dict"

  val VcfExtension = ".vcf"

  val GzExtension = ".gz"

  val TbiExtension = ".tbi"

  val VcfGzExtension = s"$VcfExtension$GzExtension"

  val VcfGzTbiExtension = s"$VcfGzExtension$TbiExtension"

}
