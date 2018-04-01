package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.ExecutionContext

class CommandDispatch(webClient: ClioWebClient, ioUtil: IoUtil) {

  def dispatch(command: ClioCommand)(
    implicit ec: ExecutionContext
  ): Source[Json, NotUsed] = {
    (command match {
      case deliverCommand: DeliverWgsCram  => new DeliverWgsCramExecutor(deliverCommand)
      case deliverCommand: DeliverArrays   => new DeliverArraysExecutor(deliverCommand)
      case addCommand: AddCommand[_]       => new AddExecutor(addCommand)
      case moveCommand: MoveCommand[_]     => new MoveExecutor(moveCommand)
      case deleteCommand: DeleteCommand[_] => new DeleteExecutor(deleteCommand)
      case retrieveAndPrint: RetrieveAndPrintCommand =>
        new RetrieveAndPrintExecutor(retrieveAndPrint)
    }).execute(webClient, ioUtil)
  }
}
