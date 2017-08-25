package org.broadinstitute.clio.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import caseapp.{CommandAppWithPreCommand, RemainingArgs}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.commands.CustomArgParsers._
import org.broadinstitute.clio.client.commands.{
  CommandDispatch,
  CommandType,
  CommonOptions
}
import org.broadinstitute.clio.client.webclient.ClioWebClient

object ClioClient
    extends CommandAppWithPreCommand[CommonOptions, CommandType]
    with LazyLogging {
  override val progName = "clio-client"
  override val appName = "Clio Client"

  override val appVersion: String = ClioClientConfig.Version.value
  //added to stop Intellij from removing import
  offsetDateTimeParser

  //not sure this is the best way to pass common opts but case-app docs are spotty at best
  var commonOptions: CommonOptions = _

  override def beforeCommand(options: CommonOptions,
                             remainingArgs: Seq[String]): Unit = {
    commonOptions = options
    if (remainingArgs.nonEmpty) {
      logger.error(s"Found extra arguments: ${remainingArgs.mkString(" ")}")
      sys.exit(1)
    }
  }

  implicit val system: ActorSystem = ActorSystem("clio-client")
  import system.dispatcher
  sys.addShutdownHook({ val _ = system.terminate() })

  var webClient: ClioWebClient = new ClioWebClient(
    ClioClientConfig.ClioServer.clioServerHostName,
    ClioClientConfig.ClioServer.clioServerPort,
    ClioClientConfig.ClioServer.clioServerUseHttps
  )

  override def run(command: CommandType, remainingArgs: RemainingArgs): Unit = {
    implicit val bearerToken: OAuth2BearerToken = commonOptions.bearerToken
    val success = command match {
      case command: CommandType =>
        CommandDispatch.dispatch(webClient, command)
    }
    sys.exit(if (success) 0 else 1)
  }

}
