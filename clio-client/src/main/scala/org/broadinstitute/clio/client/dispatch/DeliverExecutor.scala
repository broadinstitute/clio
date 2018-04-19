package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.client.commands.DeliverCommand
import org.broadinstitute.clio.client.dispatch.MoveExecutor.IoOp
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.DeliverableIndex

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/**
  * Executor for "deliver" commands, which are a specialized form of "move" commands.
  *
  * Delivery executors can extend this class to add custom IO operations which
  * should be performed when moving files into a FireCloud workspace.
  */
abstract class DeliverExecutor[CI <: DeliverableIndex](
  protected val deliverCommand: DeliverCommand[CI]
)(
  implicit ec: ExecutionContext
) extends MoveExecutor(deliverCommand) {

  protected def buildDelivery(
    metadata: moveCommand.index.MetadataType,
    moveOps: immutable.Seq[IoOp]
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed]

  override def checkPreconditions(
    ioUtil: IoUtil,
    webClient: ClioWebClient
  ): Source[moveCommand.index.MetadataType, NotUsed] = {
    val baseStream = super.checkPreconditions(ioUtil, webClient)
    baseStream.flatMapConcat { metadata =>
      if (deliverCommand.force || !metadata.workspaceName.exists(
            _ != deliverCommand.workspaceName
          )) {
        Source.single(metadata)
      } else {
        Source.failed(
          new UnsupportedOperationException(
            s"Cannot deliver ${deliverCommand.index.name} to workspace '${deliverCommand.workspaceName}' " +
              s"because it has already been delivered to workspace '${metadata.workspaceName.get}'. Use --force " +
              "if you want to override this restriction."
          )
        )
      }
    }
  }

  override protected[dispatch] def buildMove(
    metadata: moveCommand.index.MetadataType,
    ioUtil: IoUtil
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed] = {

    val baseStream = super
      .buildMove(metadata, ioUtil)
      .orElse(Source.single(metadata -> immutable.Seq.empty))

    baseStream.flatMapConcat {
      case (movedMetadata, moveOps) =>
        buildDelivery(
          movedMetadata.withWorkspaceName(deliverCommand.workspaceName),
          moveOps
        )
    }
  }
}
