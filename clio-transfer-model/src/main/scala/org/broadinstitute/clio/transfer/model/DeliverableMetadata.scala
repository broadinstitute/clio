package org.broadinstitute.clio.transfer.model

trait DeliverableMetadata[M <: DeliverableMetadata[M]] extends TransferMetadata[M] {
  self: M =>
  val workspaceName: Option[String]

  def withWorkspaceName(name: String): M
}
