package org.broadinstitute.clio.client.metadata
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{IoOp, WriteOp}
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

    val dest = src.copy(
      cramPath = movedCram,
      craiPath = movedCram.map(
        cramUri => URI.create(s"$cramUri${CramExtensions.CraiExtensionAddition}")
      )
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
        Iterable(
          extractMoves(src, dest, _.preAdapterSummaryMetricsPath),
          extractMoves(src, dest, _.preAdapterDetailMetricsPath),
          extractMoves(src, dest, _.alignmentSummaryMetricsPath),
          extractMoves(src, dest, _.duplicateMetricsPath),
          extractMoves(src, dest, _.fingerprintingSummaryMetricsPath),
          extractMoves(src, dest, _.fingerprintingDetailMetricsPath),
          extractMoves(src, dest, _.gcBiasSummaryMetricsPath),
          extractMoves(src, dest, _.gcBiasDetailMetricsPath),
          extractMoves(src, dest, _.insertSizeMetricsPath),
          extractMoves(src, dest, _.qualityDistributionMetricsPath),
          extractMoves(src, dest, _.rawWgsMetricsPath),
          extractMoves(src, dest, _.readgroupAlignmentSummaryMetricsPath),
          extractMoves(src, dest, _.readgroupGcBiasSummaryMetricsPath),
          extractMoves(src, dest, _.readgroupGcBiasDetailMetricsPath),
          extractMoves(src, dest, _.baitBiasSummaryMetricsPath),
          extractMoves(src, dest, _.baitBiasDetailMetricsPath),
          extractMoves(src, dest, _.wgsMetricsPath),
          extractMoves(src, dest, _.crosscheckPath)
        ).flatten
      } else {
        Iterable.empty
      }

    (dest, ops ++ moveMetricsOps)
  }
}
