package org.broadinstitute.clio.client

import akka.actor.ActorSystem
import caseapp.{CommandAppWithPreCommand, RemainingArgs}
import org.broadinstitute.clio.client.commands.CustomArgParsers._
import org.broadinstitute.clio.client.commands.{CommandDispatch, CommandType, CommonOptions}
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.ExecutionContext

object ClioClient extends CommandAppWithPreCommand[CommonOptions, CommandType] {
  override val progName = "clio-client"
  override val appName = "Clio Client"

  override val appVersion = ClioClientConfig.Version.value
  //added to stop Intellij from removing import
  locationParser

  //not sure this is the best way to pass common opts but case-app docs are spotty at best
  var commonOptions: CommonOptions = _

  implicit val system: ActorSystem = ActorSystem("clio-client")
  import system.dispatcher
  sys.addShutdownHook({ val _ = system.terminate() })

  val webClient = new ClioWebClient(
    ClioClientConfig.ClioServer.clioServerHostName,
    ClioClientConfig.ClioServer.clioServerPort,
    ClioClientConfig.ClioServer.clioServerUseHttps
  )

  override def run(command: CommandType, remainingArgs: RemainingArgs): Unit = {
    sys.exit(
      new ClioClient(webClient, command, commonOptions.bearerToken).execute()
    )
  }

  override def beforeCommand(options: CommonOptions,
                             remainingArgs: Seq[String]): Unit = {
    commonOptions = options
  }
}


class ClioClient(webClient: ClioWebClient,
                 command: CommandType,
                 bearerToken: String)(implicit ec: ExecutionContext) {
  def execute(): Int = {
    val success = command match {
      case command: CommandType =>
        CommandDispatch.dispatch(webClient, command, bearerToken)
    }
    if (success) 0 else 1
  }
}
