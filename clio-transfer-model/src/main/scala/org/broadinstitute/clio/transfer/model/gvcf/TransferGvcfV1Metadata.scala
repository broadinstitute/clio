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

  override def pathsToDelete: Seq[String] =
    Seq.concat(gvcfPath, gvcfIndexPath)

  override def mapMove(
    pathMapper: Option[String] => Option[String]
  ): TransferGvcfV1Metadata = {
    this.copy(
      gvcfPath = pathMapper(gvcfPath),
      gvcfIndexPath = pathMapper(gvcfIndexPath),
      gvcfMetricsPath = pathMapper(gvcfMetricsPath)
    )
  }

  override def markDeleted(deletionNote: String): TransferGvcfV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )
}
