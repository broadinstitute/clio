package org.broadinstitute.clio.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.commands.CustomArgParsers._
import org.broadinstitute.clio.client.commands.{
  CommandDispatch,
  CommandType,
  CommonOptions
}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import caseapp._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ClioClient
    extends CommandAppWithPreCommand[CommonOptions, CommandType]
    with LazyLogging {
  override val progName = "clio-client"
  override val appName = "Clio Client"

  override val appVersion: String = ClioClientConfig.Version.value

  //not sure this is the best way to pass common opts but case-app docs are spotty at best
  var commonOptions: CommonOptions = _
  UUIDParser

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

    val commandDispatch = new CommandDispatch(webClient, IoUtil)

    val client = new ClioClient(commandDispatch)

    client.execute(command) onComplete {
      case Success(_) => sys.exit(0)
      case Failure(_) => sys.exit(1)
    }
  }
}

class ClioClient(commandDispatch: CommandDispatch) {

  def execute(command: CommandType)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    commandDispatch.dispatch(command)
  }

}
