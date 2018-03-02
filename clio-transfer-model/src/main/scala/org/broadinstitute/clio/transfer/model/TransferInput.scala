package org.broadinstitute.clio.transfer.model

import org.broadinstitute.clio.util.model.DocumentStatus

trait TransferInput[IP <: TransferInput[IP]] { self: IP =>

  /**
    * Status of the document represented by this metadata,
    * primarily used to flag deletion.
    */
  val documentStatus: Option[DocumentStatus]

  /**
    * Return a copy of this object with given document status.
    */
  def withDocumentStatus(documentStatus: Option[DocumentStatus]): IP
}
