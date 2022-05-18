package org.broadinstitute.clio.transfer.model.gvcf

import org.broadinstitute.clio.transfer.model.QueryInput
import org.broadinstitute.clio.util.model.{
  DataType,
  DocumentStatus,
  Location,
  RegulatoryDesignation
}

case class GvcfQueryInput(
  documentStatus: Option[DocumentStatus] = None,
  location: Option[Location] = None,
  project: Option[String] = None,
  dataType: Option[DataType] = None,
  sampleAlias: Option[String] = None,
  version: Option[Int] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  workspaceName: Option[String] = None,
  billingProject: Option[String] = None
) extends QueryInput[GvcfQueryInput] {

  def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): GvcfQueryInput =
    this.copy(
      documentStatus = documentStatus
    )
}
