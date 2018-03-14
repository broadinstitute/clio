package org.broadinstitute.clio.transfer.model

trait DeliverableIndex extends TransferIndex {
  type MetadataType <: DeliverableMetadata[MetadataType]
}
