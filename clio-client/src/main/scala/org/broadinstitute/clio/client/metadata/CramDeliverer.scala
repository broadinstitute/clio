package org.broadinstitute.clio.client.metadata
import java.net.URI

import better.files.File
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{IoOp, MoveOp, WriteOp}
import org.broadinstitute.clio.transfer.model.wgscram.{CramExtensions, CramMetadata}
import org.broadinstitute.clio.util.model.RegulatoryDesignation

class CramDeliverer extends MetadataMover[CramMetadata] {
  override protected def moveMetadata(
    src: CramMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (CramMetadata, Iterable[IoOp]) = {
    val movedCram = src.cramPath.map(
      MetadataMover
        .buildFilePath(_, destination, newBasename.map(_ + CramExtensions.CramExtension))
    )

    lazy val oldBaseName =
      src.cramPath.map(File(_).name.replace(CramExtensions.CramExtension, ""))

    def makeDestMetrics(srcMetric: Option[URI]): Option[URI] = {
      srcMetric.map { metric =>
        val srcMetricFileName = File(metric).name
        val destMetricFileName = (for {
          nbn <- newBasename
          obn <- oldBaseName
        } yield {
          srcMetricFileName.replace(obn, nbn)
        }).getOrElse(srcMetricFileName)
        destination.resolve(destMetricFileName)
      }
    }

    val dest = src.copy(
      cramPath = movedCram,
      craiPath = movedCram.map(
        cramUri => URI.create(s"$cramUri${CramExtensions.CraiExtensionAddition}")
      ),
      preAdapterSummaryMetricsPath = makeDestMetrics(src.preAdapterSummaryMetricsPath),
      preAdapterDetailMetricsPath = makeDestMetrics(src.preAdapterDetailMetricsPath),
      alignmentSummaryMetricsPath = makeDestMetrics(src.alignmentSummaryMetricsPath),
      duplicateMetricsPath = makeDestMetrics(src.duplicateMetricsPath),
      fingerprintingSummaryMetricsPath =
        makeDestMetrics(src.fingerprintingSummaryMetricsPath),
      fingerprintingDetailMetricsPath =
        makeDestMetrics(src.fingerprintingDetailMetricsPath),
      gcBiasSummaryMetricsPath = makeDestMetrics(src.gcBiasSummaryMetricsPath),
      gcBiasDetailMetricsPath = makeDestMetrics(src.gcBiasDetailMetricsPath),
      insertSizeMetricsPath = makeDestMetrics(src.insertSizeMetricsPath),
      qualityDistributionMetricsPath = makeDestMetrics(src.qualityDistributionMetricsPath),
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
      crosscheckPath = makeDestMetrics(src.crosscheckPath),
    )

    val writeMd5Op = for {
      md5 <- src.cramMd5
      cram <- movedCram
    } yield {
      WriteOp(md5.name, URI.create(s"$cram${CramExtensions.Md5ExtensionAddition}"))
    }

    val ops = Iterable(
      extractMoves(src, dest, _.cramPath),
      extractMoves(src, dest, _.craiPath),
      writeMd5Op.toIterable
    ).flatten

    val moveMetricsOps =
      if (src.regulatoryDesignation
            .exists(_.equals(RegulatoryDesignation.ResearchOnly))) {
        src.sampleLevelMetrics.flatten
          .zip(src.sampleLevelMetrics.flatMap(makeDestMetrics))
          .map(MoveOp.tupled)
      } else {
        Iterable.empty
      }

    (dest, ops ++ moveMetricsOps)
  }
}
