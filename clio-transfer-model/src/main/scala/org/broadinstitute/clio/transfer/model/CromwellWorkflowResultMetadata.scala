package org.broadinstitute.clio.transfer.model

import java.util.UUID

trait CromwellWorkflowResultMetadata[M <: CromwellWorkflowResultMetadata[M]]
    extends Metadata[M] {
  self: M =>
  val cromwellId: Option[UUID]
}
