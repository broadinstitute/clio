package org.broadinstitute.clio.client

import org.broadinstitute.clio.client.commands.{CommandDispatch, CommandType}
import org.broadinstitute.clio.client.commands.Commands._
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.client.util.IoUtil

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import caseapp._
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}

object ClioClient
    extends CommandAppWithPreCommand[CommonOptions, CommandType]
    with LazyLogging {
  override val progName = "clio-client"
  override val appName = "Clio Client"

  override val appVersion: String = ClioClientConfig.Version.value

  //not sure this is the best way to pass common opts but case-app docs are spotty at best
  var commonOptions: CommonOptions = _

  override def beforeCommand(options: CommonOptions,
                             remainingArgs: Seq[String]): Unit = {
    commonOptions = options
    checkRemainingArgs(remainingArgs)
  }

  private def checkRemainingArgs(remainingArgs: Seq[String]): Unit = {
    if (remainingArgs.nonEmpty) {
      logger.error(s"Found extra arguments: ${remainingArgs.mkString(" ")}")
      sys.exit(1)
    }
  }

  implicit val system: ActorSystem = ActorSystem(progName)
  import system.dispatcher
  sys.addShutdownHook({ val _ = system.terminate() })

  val webClient: ClioWebClient = new ClioWebClient(
    ClioClientConfig.ClioServer.clioServerHostName,
    ClioClientConfig.ClioServer.clioServerPort,
    ClioClientConfig.ClioServer.clioServerUseHttps,
    ClioClientConfig.responseTimeout
  )

  override def run(command: CommandType, remainingArgs: RemainingArgs): Unit = {

    checkRemainingArgs(remainingArgs.args)

    implicit val bearerToken: OAuth2BearerToken = commonOptions.bearerToken

    val commandDispatch = new CommandDispatch(webClient, IoUtil)

    commandDispatch.dispatch(command) onComplete {
      case Success(_) => sys.exit(0)
      case Failure(_) => sys.exit(1)
    }
  }
}
