package org.broadinstitute.clio.transfer.model.wgscram

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.QueryInput
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation
}

case class WgsCramQueryInput(
  documentStatus: Option[DocumentStatus] = None,
  location: Option[Location] = None,
  project: Option[String] = None,
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
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  notes: Option[String] = None
) extends QueryInput[WgsCramQueryInput] {

  def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): WgsCramQueryInput =
    this.copy(
      documentStatus = documentStatus
    )
}