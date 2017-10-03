package org.broadinstitute.clio.transfer.model.gvcf

import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.DocumentStatus

case class TransferGvcfV1Metadata(analysisDate: Option[OffsetDateTime] = None,
                                  contamination: Option[Float] = None,
                                  documentStatus: Option[DocumentStatus] = None,
                                  gvcfMd5: Option[String] = None,
                                  gvcfPath: Option[String] = None,
                                  gvcfSize: Option[Long] = None,
                                  gvcfIndexPath: Option[String] = None,
                                  gvcfMetricsPath: Option[String] = None,
                                  pipelineVersion: Option[String] = None,
                                  notes: Option[String] = None)
    extends TransferMetadata[TransferGvcfV1Metadata] {

  override def pathsToMove: Seq[String] =
    Seq.concat(gvcfPath, gvcfIndexPath, gvcfMetricsPath)

  override def rebaseOnto(destination: String): TransferGvcfV1Metadata = {
    assert(destination.endsWith("/"))

    this.copy(
      gvcfPath = gvcfPath.map(rebaseOnto(_, destination)),
      gvcfIndexPath = gvcfIndexPath.map(rebaseOnto(_, destination)),
      gvcfMetricsPath = gvcfMetricsPath.map(rebaseOnto(_, destination))
    )
  }

  override def setSinglePath(destination: String): TransferGvcfV1Metadata = {
    assert(pathsToMove.length == 1)

    // Since we know only one path is defined, we can map-assign all of the possible
    // paths to the destination, knowing only one change will actually happen.
    this.copy(
      gvcfPath = gvcfPath.map(_ => destination),
      gvcfIndexPath = gvcfIndexPath.map(_ => destination),
      gvcfMetricsPath = gvcfMetricsPath.map(_ => destination)
    )
  }
}
