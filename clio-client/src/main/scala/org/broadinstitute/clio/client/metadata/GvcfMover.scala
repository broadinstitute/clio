package org.broadinstitute.clio.client.metadata
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.MoveOp
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfExtensions, GvcfMetadata}

class GvcfMover extends MetadataMover[GvcfMetadata] {
  override protected def moveMetadata(
    src: GvcfMetadata,
    destination: URI,
    newBasename: Option[String],
    undeliver: Boolean
  ): (GvcfMetadata, Iterable[MoveOp]) = {
    val dest = src.copy(
      gvcfPath = src.gvcfPath.map(
        MetadataMover.buildFilePath(
          _,
          destination,
          newBasename.map(_ + GvcfExtensions.GvcfExtension)
        )
      ),
      gvcfIndexPath = src.gvcfIndexPath.map(
        MetadataMover.buildFilePath(
          _,
          destination,
          newBasename.map(_ + GvcfExtensions.IndexExtension)
        )
      ),
      gvcfSummaryMetricsPath = src.gvcfSummaryMetricsPath.map(
        MetadataMover.buildFilePath(
          _,
          destination,
          newBasename.map(_ + GvcfExtensions.SummaryMetricsExtension)
        )
      ),
      gvcfDetailMetricsPath = src.gvcfDetailMetricsPath.map(
        MetadataMover.buildFilePath(
          _,
          destination,
          newBasename.map(_ + GvcfExtensions.DetailMetricsExtension)
        )
      )
    )

    val ops = Seq[GvcfMetadata => Iterable[URI]](
      _.gvcfPath,
      _.gvcfIndexPath,
      _.gvcfSummaryMetricsPath,
      _.gvcfDetailMetricsPath
    ).flatMap(extractMoves(src, dest, _))

    (dest, ops)
  }
}
