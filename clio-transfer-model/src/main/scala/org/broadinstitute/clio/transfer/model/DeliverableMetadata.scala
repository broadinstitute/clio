package org.broadinstitute.clio.transfer.model

trait DeliverableMetadata[M <: DeliverableMetadata[M]] extends Metadata[M] {
  self: M =>
  val workspaceName: Option[String]

  def withWorkspace(name: String, billingProject: String): M
}
