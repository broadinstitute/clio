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
class DeliverExecutor[CI <: DeliverableIndex](
  protected val deliverCommand: DeliverCommand[CI]
)(
  implicit ec: ExecutionContext
) extends MoveExecutor(deliverCommand) {

  override def checkPreconditions(
    ioUtil: IoUtil,
    webClient: ClioWebClient
  ): Source[moveCommand.index.MetadataType, NotUsed] = {
    val baseStream = super.checkPreconditions(ioUtil, webClient)
    val newWorkspace = deliverCommand.workspaceName

    baseStream.flatMapConcat { metadata =>
      metadata.workspaceName
        .filterNot(n => deliverCommand.force || n.isEmpty || n == newWorkspace)
        .fold(Source.single(metadata)) { existingWorkspace =>
          Source.failed(
            new UnsupportedOperationException(
              s"Cannot deliver $prettyKey to workspace '$newWorkspace'" +
                s" because it has already been delivered to workspace '$existingWorkspace'." +
                " Use --force if you want to override this restriction."
            )
          )
        }
    }
  }

  override protected[dispatch] def buildMove(
    metadata: moveCommand.index.MetadataType
  ): Source[(moveCommand.index.MetadataType, immutable.Seq[IoOp]), NotUsed] = {
    super.buildMove(metadata).map {
      case (m, ops) =>
        (
          m.withWorkspace(deliverCommand.workspaceName, deliverCommand.billingProject),
          ops
        )
    }
  }
}
