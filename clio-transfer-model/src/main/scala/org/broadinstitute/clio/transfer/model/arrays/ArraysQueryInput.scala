package org.broadinstitute.clio.transfer.model.arrays

import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.QueryInput
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation
}

/* The Key and Metadata fields that can be queried.
 */
case class ArraysQueryInput(
  /*
   * Key fields in getUrlSegments() order
   */
  location: Option[Location] = None,
  chipwellBarcode: Option[Symbol] = None,
  version: Option[Int] = None,
  /*
   * Rest of QueryInput fields in lexicographic order
   */
  cromwellId: Option[UUID] = None,
  documentStatus: Option[DocumentStatus] = None,
  notes: Option[String] = None,
  pipelineVersion: Option[Symbol] = None,
  project: Option[String] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  sampleAlias: Option[String] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workspaceName: Option[String] = None,
  billingProject: Option[String] = None
) extends QueryInput[ArraysQueryInput] {

  def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): ArraysQueryInput =
    this.copy(
      documentStatus = documentStatus
    )
}
