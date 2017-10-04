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

  override def moveAllInto(destination: String): TransferGvcfV1Metadata = {
    if (!destination.endsWith("/")) {
      throw new Exception("Arguments to `moveAllInto` must end with '/'")
    }

    this.copy(
      gvcfPath = gvcfPath.map(moveInto(_, destination)),
      gvcfIndexPath = gvcfIndexPath.map(moveInto(_, destination)),
      gvcfMetricsPath = gvcfMetricsPath.map(moveInto(_, destination))
    )
  }

  override def setSinglePath(destination: String): TransferGvcfV1Metadata = {
    if (pathsToMove.length != 1) {
      throw new Exception(
        "`setSinglePath` called on metadata with more than one registered path"
      )
    }

    // Since we know only one path is defined, we can map-assign all of the possible
    // paths to the destination, knowing only one change will actually happen.
    this.copy(
      gvcfPath = gvcfPath.map(_ => destination),
      gvcfIndexPath = gvcfIndexPath.map(_ => destination),
      gvcfMetricsPath = gvcfMetricsPath.map(_ => destination)
    )
  }
}
