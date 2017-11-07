package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import com.google.auth.oauth2.OAuth2Credentials

import scala.concurrent.{ExecutionContext, Future}

class CommandDispatch(webClient: ClioWebClient, ioUtil: IoUtil)(
  implicit credentials: OAuth2Credentials
) {

  def dispatch(
    command: ClioCommand
  )(implicit ec: ExecutionContext): Future[_] = {
    val executor = command match {
      case addCommand: AddCommand[_] =>
        new AddExecutor(addCommand)
      case moveCommand: MoveCommand[_] =>
        new MoveExecutor(moveCommand)
      case deleteCommand: DeleteCommand[_] =>
        new DeleteExecutor(deleteCommand)
      case deliverCommand: DeliverWgsCram =>
        new DeliverWgsCramExecutor(deliverCommand)
      case retrieveAndPrint: RetrieveAndPrintCommand =>
        new RetrieveAndPrintExecutor(retrieveAndPrint)
    }

    executor.execute(webClient, ioUtil)
  }
}
