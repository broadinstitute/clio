package org.broadinstitute.clio.transfer.model.gvcf

import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation
}

case class TransferGvcfV1QueryInput(
  documentStatus: Option[DocumentStatus] = None,
  location: Option[Location] = None,
  project: Option[String] = None,
  sampleAlias: Option[String] = None,
  version: Option[Int] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None
)
