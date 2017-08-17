package org.broadinstitute.clio.client

import org.broadinstitute.clio.client.commands.CommandDispatch
import org.broadinstitute.clio.client.parser.{BaseArgs, BaseParser}
import org.broadinstitute.clio.client.webclient.ClioWebClient

import akka.actor.ActorSystem

import scala.concurrent.ExecutionContext

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

  val parser = new BaseParser

  def execute(args: Array[String])(implicit ec: ExecutionContext): Int = {
    val success = parser.parse(args, BaseArgs()) match {
      case Some(config) => CommandDispatch.dispatch(webClient, config)
      case None         => false
    }
    if (success) 0 else 1
  }

}
