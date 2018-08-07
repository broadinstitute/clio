package org.broadinstitute.clio.client.metadata
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.MoveOp
import org.broadinstitute.clio.transfer.model.wgscram.{CramExtensions, CramMetadata}

class CramMover extends MetadataMover[CramMetadata] {
  override protected def moveMetadata(
    src: CramMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (CramMetadata, Iterable[MoveOp]) = {
    import MetadataMover.buildFilePath

    val movedCram = src.cramPath.map(
      buildFilePath(_, destination, newBasename.map(_ + CramExtensions.CramExtension))
    )

    val dest = src.copy(
      cramPath = movedCram,
      craiPath = movedCram.map { cramUri =>
        // DSDEGP-1715: We've settled on '.cram.crai' as the extension and
        // want to fixup files with just '.crai' when possible.
        URI.create(s"$cramUri${CramExtensions.CraiExtensionAddition}")
      },
      logPath = src.logPath.map(buildFilePath(_, destination)),
      analysisFilesTxtPath = src.analysisFilesTxtPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.AnalysisFilesTxtExtension)
        )
      ),
      preAdapterDetailMetricsPath = src.preAdapterDetailMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.PreAdapterDetailMetricsExtension)
        )
      ),
      preAdapterSummaryMetricsPath = src.preAdapterSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.PreAdapterSummaryMetricsExtension)
        )
      ),
      alignmentSummaryMetricsPath = src.alignmentSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.AlignmentSummaryMetricsExtension)
        )
      ),
      bamValidationReportPath = src.bamValidationReportPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.BamValidationReportExtension)
        )
      ),
      preBqsrDepthSmPath = src.preBqsrDepthSmPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.PreBqsrDepthSMExtension)
        )
      ),
      preBqsrSelfSmPath = src.preBqsrSelfSmPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.PreBqsrSelfSMExtension)
        )
      ),
      preBqsrLogPath = src.preBqsrLogPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.PreBqsrLogExtension)
        )
      ),
      cramValidationReportPath = src.cramValidationReportPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.CramValidationReportExtension)
        )
      ),
      crosscheckPath = src.crosscheckPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.CrossCheckExtension)
        )
      ),
      duplicateMetricsPath = src.duplicateMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.DuplicateMetricsExtension)
        )
      ),
      fingerprintPath = src.fingerprintPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.FingerprintVcfExtension)
        )
      ),
      fingerprintVcfPath = src.fingerprintVcfPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.FingerprintVcfExtension)
        )
      ),
      fingerprintingDetailMetricsPath = src.fingerprintingDetailMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.FingerprintingDetailMetricsExtension)
        )
      ),
      fingerprintingSummaryMetricsPath = src.fingerprintingSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.FingerprintingSummaryMetricsExtension)
        )
      ),
      gcBiasPdfPath = src.gcBiasPdfPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.GcBiasPdfExtension)
        )
      ),
      gcBiasDetailMetricsPath = src.gcBiasDetailMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.GcBiasDetailMetricsExtension)
        )
      ),
      gcBiasSummaryMetricsPath = src.gcBiasSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.GcBiasSummaryMetricsExtension)
        )
      ),
      insertSizeHistogramPath = src.insertSizeHistogramPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.InsertSizeHistogramPdfExtension)
        )
      ),
      insertSizeMetricsPath = src.insertSizeMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.InsertSizeMetricsExtension)
        )
      ),
      qualityDistributionPdfPath = src.qualityDistributionPdfPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.QualityDistributionPdfExtension)
        )
      ),
      qualityDistributionMetricsPath = src.qualityDistributionMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.QualityDistributionMetricsExtension)
        )
      ),
      rawWgsMetricsPath = src.rawWgsMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.RawWgsMetricsExtension)
        )
      ),
      readgroupAlignmentSummaryMetricsPath = src.readgroupAlignmentSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.ReadGroupAlignmentMetricsExtension)
        )
      ),
      readgroupGcBiasPdfPath = src.readgroupGcBiasPdfPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.ReadGroupGcBiasPdfExtension)
        )
      ),
      readgroupGcBiasDetailMetricsPath = src.readgroupGcBiasDetailMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.ReadGroupGcBiasDetailMetricsExtension)
        )
      ),
      readgroupGcBiasSummaryMetricsPath = src.readgroupGcBiasSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.ReadGroupGcBiasSummaryMetricsExtension)
        )
      ),
      recalDataPath = src.recalDataPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.RecalDataExtension)
        )
      ),
      baitBiasDetailMetricsPath = src.baitBiasDetailMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.BaitBiasDetailMetricsExension)
        )
      ),
      baitBiasSummaryMetricsPath = src.baitBiasSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.BaitBiasDetailMetricsExension)
        )
      ),
      wgsMetricsPath = src.wgsMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + CramExtensions.WgsMetricsExension)
        )
      ),
      readgroupLevelMetricsFiles =
        src.readgroupLevelMetricsFiles.map(_.map(buildFilePath(_, destination)))
    )

    val ops = Seq[CramMetadata => Iterable[URI]](
      _.cramPath,
      _.craiPath,
      _.logPath,
      _.analysisFilesTxtPath,
      _.preAdapterDetailMetricsPath,
      _.preAdapterSummaryMetricsPath,
      _.alignmentSummaryMetricsPath,
      _.bamValidationReportPath,
      _.preBqsrDepthSmPath,
      _.preBqsrSelfSmPath,
      _.preBqsrLogPath,
      _.cramValidationReportPath,
      _.crosscheckPath,
      _.duplicateMetricsPath,
      _.fingerprintPath,
      _.fingerprintVcfPath,
      _.fingerprintingDetailMetricsPath,
      _.fingerprintingSummaryMetricsPath,
      _.gcBiasPdfPath,
      _.gcBiasDetailMetricsPath,
      _.gcBiasSummaryMetricsPath,
      _.insertSizeHistogramPath,
      _.insertSizeMetricsPath,
      _.qualityDistributionPdfPath,
      _.qualityDistributionMetricsPath,
      _.rawWgsMetricsPath,
      _.readgroupAlignmentSummaryMetricsPath,
      _.readgroupGcBiasPdfPath,
      _.readgroupGcBiasDetailMetricsPath,
      _.readgroupGcBiasSummaryMetricsPath,
      _.recalDataPath,
      _.baitBiasDetailMetricsPath,
      _.baitBiasSummaryMetricsPath,
      _.wgsMetricsPath,
      _.readgroupLevelMetricsFiles.toIterable.flatten
    ).flatMap(extractMoves(src, dest, _))

    (dest, ops)
  }
}
