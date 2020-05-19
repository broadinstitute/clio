package org.broadinstitute.clio.transfer.model.bam

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.QueryInput
import org.broadinstitute.clio.util.model.{
  DataType,
  DocumentStatus,
  Location,
  RegulatoryDesignation
}

case class BamQueryInput(
  documentStatus: Option[DocumentStatus] = None,
  location: Option[Location] = None,
  project: Option[String] = None,
  dataType: Option[DataType] = None,
  sampleAlias: Option[String] = None,
  version: Option[Int] = None,
  pipelineVersion: Option[Symbol] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  cramMd5: Option[Symbol] = None,
  cramSize: Option[Long] = None,
  cramPath: Option[URI] = None,
  cromwellId: Option[UUID] = None,
  readgroupMd5: Option[Symbol] = None,
  workspaceName: Option[String] = None,
  billingProject: Option[String] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  notes: Option[String] = None
) extends QueryInput[BamQueryInput] {

  def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): BamQueryInput =
    this.copy(
      documentStatus = documentStatus
    )
}
