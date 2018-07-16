package org.broadinstitute.clio.transfer.model.arrays

import org.broadinstitute.clio.transfer.model.FileExtensions

/**
  *  Extensions for files in the arrays index.
  */
object ArraysExtensions extends FileExtensions {

  val BpmExtension = ".bpm"

  val EgtExtension = ".egt"

  val IdatExtension = ".idat"

  val GrnIdatExtension = s"_Grn$IdatExtension"

  val RedIdatExtension = s"_Red$IdatExtension"

  val ThresholdsExtension = ".thresholds"

  val GtcExtension = ".gtc"

  val EgtThresholdsTxtExtension = s"$EgtExtension$ThresholdsExtension$TxtExtension"

  val FingerprintingSummaryMetricsExtension = ".fingerprinting_summary_metrics"

  val FingerprintingDetailMetricsExtension = ".fingerprinting_detail_metrics"

  val GenotypeConcordanceSummaryMetricsExtension = ".genotype_concordance_summary_metrics"

  val GenotypeConcordanceDetailMetricsExtension = ".genotype_concordance_detail_metrics"

  val GenotypeConcordanceContingencyMetricsExtension =
    ".genotype_concordance_contingency_metrics"

  val VariantCallingSummaryMetricsExtension = ".arrays_variant_calling_summary_metrics"

  val VariantCallingDetailMetricsExtension = ".arrays_variant_calling_detail_metrics"
}
