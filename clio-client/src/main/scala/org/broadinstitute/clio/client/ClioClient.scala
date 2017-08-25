package org.broadinstitute.clio.client

import org.broadinstitute.clio.client.commands.CommandDispatch
import org.broadinstitute.clio.client.parser.{BaseArgs, BaseParser}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import akka.actor.ActorSystem
import org.broadinstitute.clio.client.util.IoUtil

import scala.concurrent.{Await, ExecutionContext}

object ClioClient extends App {
  implicit val system: ActorSystem = ActorSystem("clio-client")
  import system.dispatcher
  sys.addShutdownHook({ val _ = system.terminate() })

  val webClient = new ClioWebClient(
    ClioClientConfig.ClioServer.clioServerHostName,
    ClioClientConfig.ClioServer.clioServerPort,
    ClioClientConfig.ClioServer.clioServerUseHttps
  )
  val client = new ClioClient(webClient)

  System.exit(client.execute(args))
}

class ClioClient(val webClient: ClioWebClient) {

  implicit val ioUtil: IoUtil = IoUtil

  def execute(args: Array[String])(implicit ec: ExecutionContext): Int = {
    val parser: BaseParser = new BaseParser
    val success: Boolean = parser.parse(args, BaseArgs()) match {
      case Some(config) =>
        Await.result(
          CommandDispatch.dispatch(webClient, config),
          ClioClientConfig.responseTimeout
        )
      case None => false
    }
    if (success) 0 else 1
  }

}
