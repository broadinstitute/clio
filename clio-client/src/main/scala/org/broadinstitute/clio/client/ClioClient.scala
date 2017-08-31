package org.broadinstitute.clio.client

import org.broadinstitute.clio.client.commands.CommandDispatch
import org.broadinstitute.clio.client.parser.{BaseArgs, BaseParser}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.clio.client.util.IoUtil

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ClioClient extends App {
  implicit val system: ActorSystem = ActorSystem("clio-client")
  import system.dispatcher
  sys.addShutdownHook({ val _ = system.terminate() })

  val webClient = new ClioWebClient(
    ClioClientConfig.ClioServer.clioServerHostName,
    ClioClientConfig.ClioServer.clioServerPort,
    ClioClientConfig.ClioServer.clioServerUseHttps
  )
  val commandDispatch = new CommandDispatch(webClient, IoUtil)

  val client = new ClioClient(commandDispatch)

  client.execute(args) onComplete {
    case Success(_) => System.exit(0)
    case Failure(_) => System.exit(1)
  }
}

class ClioClient(commandDispatch: CommandDispatch) {

  def execute(
      args: Array[String]
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    val parser: BaseParser = new BaseParser
    parser.parse(args, BaseArgs()) match {
      case Some(config) =>
        commandDispatch.dispatch(config)
      case None =>
        Future.failed(new Exception("Could not parse arguments"))
    }
  }

}
