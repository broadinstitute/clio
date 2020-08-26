package org.broadinstitute.clio.client.metadata

import java.net.URI

import better.files.File
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{IoOp, MoveOp}
import org.broadinstitute.clio.transfer.model.cram.{CramExtensions, CramMetadata}

case class CramDeliverer(deliverSampleMetrics: Boolean)
    extends MetadataMover[CramMetadata] {

  override protected def moveMetadata(
    src: CramMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (CramMetadata, Iterable[IoOp]) = {

    if (deliverSampleMetrics) {
      if (src.regulatoryDesignation.exists(_.isClinical)) {
        throw new RuntimeException(
          s"Cannot (un)deliver sample level metrics for clinical sample $src"
        )
      }
      lazy val oldBaseName =
        src.cramPath.map(
          p => File(p.getPath).name.replace(CramExtensions.CramExtension, "")
        )

      def makeDestMetrics(srcMetric: Option[URI]): Option[URI] = {
        srcMetric.map { metric =>
          val srcMetricFileName = File(metric.getPath).name
          val destMetricFileName = (for {
            nbn <- newBasename
            obn <- oldBaseName
          } yield {
            srcMetricFileName.replace(obn, nbn)
          }).getOrElse(srcMetricFileName)
          destination.resolve(destMetricFileName)
        }
      }

      val moveMetricsOps =
        src.sampleLevelMetrics.flatten
          .zip(src.sampleLevelMetrics.flatMap(makeDestMetrics))
          .map(MoveOp.tupled)

      val movedMetricsDest = src.copy(
        preAdapterSummaryMetricsPath = makeDestMetrics(src.preAdapterSummaryMetricsPath),
        preAdapterDetailMetricsPath = makeDestMetrics(src.preAdapterDetailMetricsPath),
        alignmentSummaryMetricsPath = makeDestMetrics(src.alignmentSummaryMetricsPath),
        fingerprintingSummaryMetricsPath =
          makeDestMetrics(src.fingerprintingSummaryMetricsPath),
        duplicateMetricsPath = makeDestMetrics(src.duplicateMetricsPath),
        gcBiasSummaryMetricsPath = makeDestMetrics(src.gcBiasSummaryMetricsPath),
        gcBiasDetailMetricsPath = makeDestMetrics(src.gcBiasDetailMetricsPath),
        hybridSelectionMetricsPath = makeDestMetrics(src.hybridSelectionMetricsPath),
        insertSizeMetricsPath = makeDestMetrics(src.insertSizeMetricsPath),
        qualityDistributionMetricsPath =
          makeDestMetrics(src.qualityDistributionMetricsPath),
        rawWgsMetricsPath = makeDestMetrics(src.rawWgsMetricsPath),
        readgroupAlignmentSummaryMetricsPath =
          makeDestMetrics(src.readgroupAlignmentSummaryMetricsPath),
        readgroupGcBiasSummaryMetricsPath =
          makeDestMetrics(src.readgroupGcBiasSummaryMetricsPath),
        readgroupGcBiasDetailMetricsPath =
          makeDestMetrics(src.readgroupGcBiasDetailMetricsPath),
        baitBiasSummaryMetricsPath = makeDestMetrics(src.baitBiasSummaryMetricsPath),
        baitBiasDetailMetricsPath = makeDestMetrics(src.baitBiasDetailMetricsPath),
        wgsMetricsPath = makeDestMetrics(src.wgsMetricsPath),
        crosscheckPath = makeDestMetrics(src.crosscheckPath)
      )
      (movedMetricsDest, moveMetricsOps)
    } else {
      (src, Iterable.empty)
    }
  }
}
