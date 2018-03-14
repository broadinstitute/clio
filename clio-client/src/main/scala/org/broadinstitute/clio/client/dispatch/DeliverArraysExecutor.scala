package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.transfer.model.arrays.TransferArraysV1Metadata

import scala.concurrent.{ExecutionContext, Future}

/**
  * Special-purpose CLP for delivering arrays to FireCloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic:
  *
  *   1. Writes the arrays md5 value to file at the target path
  *   2. Records the workspace name in the metadata for the delivered arrays
  */
class DeliverArraysExecutor(deliverCommand: DeliverArrays)
    extends DeliverExecutor(deliverCommand) {

  override def customMetadataOperations(metadata: TransferArraysV1Metadata, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[Unit] = ???
}
