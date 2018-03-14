package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands.DeliverCommand
import org.broadinstitute.clio.transfer.model.DeliverableIndex

/**
  * Special-purpose CLP for delivering crams to FireCloud workspaces.
  *
  * Wraps the move CLP with extra IO / upsert logic:
  *
  *   1. Writes the cram md5 value to file at the target path
  *   2. Records the workspace name in the metadata for the delivered cram
  */
abstract class DeliverExecutor[DI <: DeliverableIndex](deliverCommand: DeliverCommand[DI])
    extends MoveExecutor[DI](deliverCommand)
