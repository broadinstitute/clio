package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.client.commands.DeliverCommand
import org.broadinstitute.clio.client.dispatch.MoveExecutor.IoOp
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.transfer.model.ClioIndex

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/**
  * Executor for "deliver" commands, which are a specialized form of "move" commands.
  *
  * Delivery executors can extend this class to add custom IO operations which
  * should be performed when moving files into a FireCloud workspace.
  */
abstract class DeliverExecutor[CI <: ClioIndex](
  protected val deliverCommand: DeliverCommand[CI]
)(
  implicit ec: ExecutionContext
) extends MoveExecutor(deliverCommand) {

  protected def buildDelivery(
    metadata: moveCommand.index.MetadataType,
    moveOps: immutable.Seq[IoOp]
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed]

  override protected[dispatch] def buildMove(
    metadata: moveCommand.index.MetadataType,
    ioUtil: IoUtil
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed] = {

    val baseStream = super
      .buildMove(metadata, ioUtil)
      .orElse(Source.single(metadata -> immutable.Seq.empty))

    baseStream.flatMapConcat {
      case (movedMetadata, moveOps) =>
        buildDelivery(movedMetadata, moveOps)
    }
  }
}
