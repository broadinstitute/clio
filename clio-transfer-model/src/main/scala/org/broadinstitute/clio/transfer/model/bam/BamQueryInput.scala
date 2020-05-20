package org.broadinstitute.clio.transfer.model.bam

import java.net.URI
import org.broadinstitute.clio.transfer.model.QueryInput
import org.broadinstitute.clio.util.model.{DataType, DocumentStatus, Location}

case class BamQueryInput(
  documentStatus: Option[DocumentStatus] = None,
  location: Option[Location] = None,
  project: Option[String] = None,
  dataType: Option[DataType] = None,
  sampleAlias: Option[String] = None,
  version: Option[Int] = None,
  bamMd5: Option[Symbol] = None,
  bamSize: Option[Long] = None,
  bamPath: Option[URI] = None,
  workspaceName: Option[String] = None,
  billingProject: Option[String] = None,
  notes: Option[String] = None
) extends QueryInput[BamQueryInput] {

  def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): BamQueryInput =
    this.copy(
      documentStatus = documentStatus
    )
}
