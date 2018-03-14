package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}

class CommandDispatch(webClient: ClioWebClient, ioUtil: IoUtil) {

  def dispatch(
    command: ClioCommand
  )(implicit ec: ExecutionContext): Future[_] = {
    (command match {
      case deliverCommand: DeliverWgsCram  => new DeliverWgsCramExecutor(deliverCommand)
      case addCommand: AddCommand[_]       => new AddExecutor(addCommand)
      case moveCommand: MoveCommand[_]     => new MoveExecutor(moveCommand)
      case deleteCommand: DeleteCommand[_] => new DeleteExecutor(deleteCommand)
      case retrieveAndPrint: RetrieveAndPrintCommand =>
        new RetrieveAndPrintExecutor(retrieveAndPrint)
    }).execute(webClient, ioUtil)
  }
}
