package org.broadinstitute.clio.client

import org.broadinstitute.clio.client.commands.CommandDispatch
import org.broadinstitute.clio.client.parser.{BaseArgs, BaseParser}
import org.broadinstitute.clio.client.webclient.ClioWebClient

object ClioClient extends App {
  new ClioClient(webClient = new ClioWebClient()).execute(args)
}
class ClioClient(val webClient: ClioWebClient) {

  val parser = new BaseParser

  def execute(args: Array[String]): Int = {
    val success = parser.parse(args, BaseArgs()) match {
      case Some(config) => CommandDispatch.dispatch(webClient, config)
      case None         => false
    }
    if (success) 0 else 1
  }

}
