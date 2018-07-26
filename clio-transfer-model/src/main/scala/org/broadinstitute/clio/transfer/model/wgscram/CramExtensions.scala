package org.broadinstitute.clio.transfer.model.wgscram

import org.broadinstitute.clio.transfer.model.FileExtensions

/** Expected extensions for files tracked by the wgs-cram index. */
object CramExtensions extends FileExtensions {

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

  // Fingerprint VCF Extension
  val FingerprintVcfExtension = s".reference.fingerprint$VcfGzExtension"

  // Fingerprint VCF Index Extension
  val FingerprintVcfIndexExtension = s".reference.fingerprint$VcfGzTbiExtension"

  // Fingerprinting Summary File Extension
  val FingerprintingSummaryMetricsExtension = ".fingerprinting_summary_metrics"

  // Fingerprinting Details File Extension
  val FingerprintingDetailMetricsExtension = ".fingerprinting_detail_metrics"

  val AnalysisFilesTxtExtension = s".analysis_files$TxtExtension"

  val AlignmentSummaryMetricsExtension = ".alignment_summary_metrics"

  val PreAdapterSummaryMetricsExtension = ".pre_adapter_summary_metrics"

  val PreAdapterDetailMetricsExtension = ".pre_adapter_detail_metrics"

  val BamValidationReportExtension = ".bam.validation_report"

  val CramValidationReportExtension = ".cram.validation_report"

  val PreBqsrSelfSMExtension = ".preBqsr.selfSM"

  val PreBqsrDepthSMExtension = ".preBqsr.depthSM"

  val CrossCheckExtension = ".crosscheck"

  val DuplicateMetricsExtension = ".duplicate_metrics"

  val GcBiasPdfExtension = "gc_bias.pdf"

  val GcBiasSummaryMetricsExtension = ".gc_bias.summary_metrics"

  val GcBiasDetailMetricsExtension = ".gc_bias.detail_metrics"

  val InsertSizeHistogramPdfExtension = ".insert_size_histogram.pdf"

  val InsertSizeMetricsExtension = ".insert_size_metrics"

  val QualityDistributionPdfExtension = ".quality_distribution_pdf"

  val QualityDistributionMetricsExtension = ".quality_distribution_metrics"

  val RawWgsMetricsExtension = ".raw_wgs_metrics"

  val ReadGroupAlignmentMetricsExtension = ".alignment_summary_metrics"

  val ReadGroupGcBiasPdfExtension = ".gc_bias_pdf"

  val ReadGroupGcBiasSummaryMetricsExtension = ".gc_bias_summary_metrics"

  val ReadGroupGcBiasDetailMetricsExtension = ".gc_bias_detail_metrics"

  val RecalDataExtension = ".recal_data.csv"

  val BaitBiasSummaryMetricsExtension = ".bait_bias_summary_metrics"

  val BaitBiasDetailMetricsExension = ".bait_bias_detail_metrics"

  val WgsMetricsExension = ".wgs_metrics"
}
