package org.broadinstitute.clio.transfer.model.gvcf

/** Expected extensions for files tracked by the gvcf index. */
object GvcfExtensions {

  /** File extension for gvcfs. */
  val GvcfExtension = ".g.vcf.gz"

  /** File extension for gvcf indices. */
  val IndexExtension = s"$GvcfExtension.tbi"

  /** File extension for gvcf summary metrics. */
  val SummaryMetricsExtension = ".variant_calling_summary_metrics"

  /** File extension for gvcf detail metrics. */
  val DetailMetricsExtension = ".variant_calling_detail_metrics"
}
