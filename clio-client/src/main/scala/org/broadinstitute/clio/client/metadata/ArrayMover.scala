package org.broadinstitute.clio.client.metadata
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.MoveOp
import org.broadinstitute.clio.transfer.model.arrays.{ArraysExtensions, ArraysMetadata}

class ArrayMover extends MetadataMover[ArraysMetadata] {
  override protected def moveMetadata(
    src: ArraysMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (ArraysMetadata, Iterable[MoveOp]) = {
    import MetadataMover.buildFilePath

    val dest = src.copy(
      vcfPath = src.vcfPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.VcfGzExtension)
        )
      ),
      vcfIndexPath = src.vcfIndexPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.VcfGzTbiExtension)
        )
      ),
      gtcPath = src.gtcPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.GtcExtension)
        )
      ),
      // Note - the params file is named 'params.txt' - it does not get renamed.
      paramsPath = src.paramsPath.map(
        buildFilePath(
          _,
          destination
        )
      ),
      fingerprintingDetailMetricsPath = src.fingerprintingDetailMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.FingerprintingDetailMetricsExtension)
        )
      ),
      fingerprintingSummaryMetricsPath = src.fingerprintingSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.FingerprintingSummaryMetricsExtension)
        )
      ),
      genotypeConcordanceContingencyMetricsPath =
        src.genotypeConcordanceContingencyMetricsPath.map(
          buildFilePath(
            _,
            destination,
            newBasename.map(
              _ + ArraysExtensions.GenotypeConcordanceContingencyMetricsExtension
            )
          )
        ),
      genotypeConcordanceDetailMetricsPath = src.genotypeConcordanceDetailMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.GenotypeConcordanceDetailMetricsExtension)
        )
      ),
      genotypeConcordanceSummaryMetricsPath =
        src.genotypeConcordanceSummaryMetricsPath.map(
          buildFilePath(
            _,
            destination,
            newBasename.map(
              _ + ArraysExtensions.GenotypeConcordanceSummaryMetricsExtension
            )
          )
        ),
      variantCallingDetailMetricsPath = src.variantCallingDetailMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.VariantCallingDetailMetricsExtension)
        )
      ),
      variantCallingSummaryMetricsPath = src.variantCallingSummaryMetricsPath.map(
        buildFilePath(
          _,
          destination,
          newBasename.map(_ + ArraysExtensions.VariantCallingSummaryMetricsExtension)
        )
      )
    )

    val ops = Seq[ArraysMetadata => Iterable[URI]](
      _.vcfPath,
      _.vcfIndexPath,
      _.gtcPath,
      _.paramsPath,
      _.fingerprintingDetailMetricsPath,
      _.fingerprintingSummaryMetricsPath,
      _.genotypeConcordanceContingencyMetricsPath,
      _.genotypeConcordanceDetailMetricsPath,
      _.genotypeConcordanceSummaryMetricsPath,
      _.variantCallingDetailMetricsPath,
      _.variantCallingSummaryMetricsPath
    ).flatMap(extractMoves(src, dest, _))

    (dest, ops)
  }
}
