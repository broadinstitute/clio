package org.broadinstitute.clio.transfer.model

trait DeliverableIndex extends ClioIndex {
  type MetadataType <: DeliverableMetadata[MetadataType]
}
