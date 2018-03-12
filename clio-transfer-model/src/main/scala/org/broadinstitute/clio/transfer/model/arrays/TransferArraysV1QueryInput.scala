package org.broadinstitute.clio.transfer.model.arrays

import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.TransferQueryInput
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

/* The Key and Metadata fields that can be queried.
 */
case class TransferArraysV1QueryInput(
  /*
   * Key fields in getUrlSegments() order
   */
  location: Option[Location] = None,
  chipwellBarcode: Option[Symbol] = None,
  analysisVersionNumber: Option[Int] = None,
  version: Option[Int] = None,
  /*
   * Rest of QueryInput fields in lexicographic order
   */
  cromwellId: Option[UUID] = None,
  documentStatus: Option[DocumentStatus] = None,
  notes: Option[String] = None,
  pipelineVersion: Option[Symbol] = None,
  project: Option[String] = None,
  sampleAlias: Option[String] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workspaceName: Option[String] = None
) extends TransferQueryInput[TransferArraysV1QueryInput] {

  def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): TransferArraysV1QueryInput =
    this.copy(
      documentStatus = documentStatus
    )
}
