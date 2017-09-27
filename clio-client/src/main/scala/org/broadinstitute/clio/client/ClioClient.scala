package org.broadinstitute.clio.client

import org.broadinstitute.clio.client.commands.{ClioCommand, CommonOptions}
import org.broadinstitute.clio.client.dispatch.CommandDispatch
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import caseapp.core.{Error, RemainingArgs}
import caseapp.core.help.{Help, WithHelp}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.AuthUtil

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * The command-line entry point for the clio-client.
  *
  * We want most actual logic to happen inside this
  * object's companion class for maximum testability,
  * so this object should only handle:
  *
  *   1. Setting up and tearing down the actor system
  *      infrastructure needed by the client.
  *
  *   2. Exiting the program with appropriate return
  *      codes.
  */
object ClioClient extends LazyLogging {

  /**
    * Model representing termination of the client before
    * it even gets to running a sub-command.
    *
    * This could be because of an input error, or because
    * the user asked for help / usage.
    *
    * We model the two cases differently so we can exit with
    * different return codes accordingly.
    */
  sealed trait EarlyReturn
  final case class ParsingError(error: Error) extends EarlyReturn
  final case class AuthError(cause: Throwable) extends EarlyReturn
  final case class UsageOrHelpAsked(message: String) extends EarlyReturn

  val progName = "clio-client"

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem(progName)
    import system.dispatcher
    sys.addShutdownHook({ val _ = system.terminate() })

    val webClient: ClioWebClient = new ClioWebClient(
      ClioClientConfig.ClioServer.clioServerHostName,
      ClioClientConfig.ClioServer.clioServerPort,
      ClioClientConfig.ClioServer.clioServerUseHttps,
      ClioClientConfig.responseTimeout
    )
    val client: ClioClient = new ClioClient(webClient, IoUtil)

    client
      .instanceMain(args)
      .fold(
        {
          case UsageOrHelpAsked(message) => {
            println(message)
            sys.exit(0)
          }
          case ParsingError(error) => {
            System.err.println(error.message)
            sys.exit(1)
          }
          case AuthError(cause) => {
            logger.error(
              "Failed to automatically generate bearer token, try manually passing one via --bearer-token",
              cause
            )
            sys.exit(1)
          }
        },
        _.onComplete {
          case Success(_) => sys.exit(0)
          case Failure(ex) => {
            logger.error("Failed to execute command", ex)
            sys.exit(1)
          }
        }
      )
  }
}

/**
  * The main clio-client application.
  *
  * Inspired by / partially copy-pasted from the
  * [[caseapp.core.app.CommandAppWithPreCommand]], which demonstrates
  * how to use caseapp's parsers to build a client with
  * subcommands sharing common options. This class
  * reimplements that class instead of extending it because
  * that class is designed to be the top-level entry point of
  * a program, with a main method and calls to sys.exit.
  *
  * Reimplementing the class also lets us tweak its logic so
  * that common options are provided to the sub-command-handling
  * method, rather than kept separate.
  *
  * @param webClient client handling HTTP communication with
  *                  the clio-server
  * @param ioUtil    utility handling all file operations,
  *                  both local and in cloud storage
  * @param ec        the execution context to use when scheduling
  *                  asynchronous events
  */
class ClioClient(webClient: ClioWebClient,
                 ioUtil: IoUtil)(implicit ec: ExecutionContext)
    extends LazyLogging {
  import ClioClient.{AuthError, EarlyReturn, ParsingError, UsageOrHelpAsked}

  /**
    * Common option messages, updated to include our program
    * name and version info.
    *
    * caseapp supports setting these through annotations, but
    * only if the values are constant strings.
    */
  private val beforeCommandHelp: Help[CommonOptions] =
    CommonOptions.help.copy(
      appName = "Clio Client",
      appVersion = ClioClientConfig.Version.value,
      progName = ClioClient.progName,
      optionsDesc = "[options] [command] [command-options]"
    )

  /** Names of all valid sub-commands. */
  private val commands: Seq[String] =
    ClioCommand.help.messagesMap.keys.toSeq

  /** Top-level help message to display on --help. */
  private val helpMessage: String =
    s"""
       |${beforeCommandHelp.help}
       |Available commands:
       |${commands.map(commandHelp).mkString("\n")}
     """.stripMargin

  /**
    * Top-level usage message to display on --usage,
    * or when users give no sub-command.
    */
  private val usageMessage: String =
    s"""
       |${beforeCommandHelp.usage}
       |Available commands:
       |  ${commands.mkString(", ")}
     """.stripMargin

  /** Help message for a specific command. */
  private def commandHelp(command: String): String = {
    ClioCommand.help
      .messagesMap(command)
      .helpMessage(s"${beforeCommandHelp.progName} [options]", command)
  }

  /** Usage message for a specific command. */
  private def commandUsage(command: String): String = {
    ClioCommand.help
      .messagesMap(command)
      .usageMessage(
        s"${beforeCommandHelp.progName} [options]",
        s"$command [command-options]"
      )
  }

  /**
    * The client's entry point.
    *
    * Note: this can't be named `main` because if it is, the compiler will
    * try to be helpful and generate a forwarder method also named `main`
    * in the companion object that will do nothing but call this method
    * (allowing this class to serve as a top-level entry-point), conflicting
    * with the main method already present in the companion object.
    */
  def instanceMain(
    args: Array[String]
  ): Either[EarlyReturn, Future[HttpResponse]] = {
    val maybeParse =
      ClioCommand.parser.withHelp
        .detailedParse(args)(CommonOptions.parser.withHelp)

    wrapError(maybeParse).flatMap {
      case (commonParse, commonArgs, maybeCommandParse) => {
        for {
          _ <- messageIfAsked(helpMessage, commonParse.help)
          _ <- messageIfAsked(usageMessage, commonParse.usage)
          commonOpts <- wrapError(commonParse.baseOrError)
          _ <- checkRemainingArgs(commonArgs)
          response <- maybeCommandParse
            .map { commandParse =>
              wrapError(commandParse)
                .flatMap(commandMain(commonOpts, _))
            }
            .getOrElse(Left(ParsingError(Error.Other(usageMessage))))
        } yield {
          response
        }
      }
    }
  }

  /**
    * Utility for wrapping a parsing error in our type infrastructure,
    * for cleaner use in for-comprehensions.
    */
  private def wrapError[X](either: Either[Error, X]): Either[EarlyReturn, X] = {
    either.left.map(ParsingError.apply)
  }

  /**
    * Utility for wrapping a help / usage message in our type
    * infrastructure, for cleaner use in for-comprehensions.
    */
  private def messageIfAsked(message: String,
                             asked: Boolean): Either[EarlyReturn, Unit] = {
    Either.cond(!asked, (), UsageOrHelpAsked(message))
  }

  private def getAccessToken(
    tokenOption: Option[OAuth2BearerToken]
  ): Either[EarlyReturn, OAuth2BearerToken] = {
    tokenOption.fold({
      AuthUtil
        .getAccessToken(ClioClientConfig.serviceAccountJson)
        .map(token => OAuth2BearerToken(token.getTokenValue))
        .left
        .map(AuthError.apply)
    })(Right(_))
  }

  /**
    * Utility for wrapping a check for extra arguments in our
    * type infrastructure, for cleaner use in for-comprehensions.
    */
  private def checkRemainingArgs(
    remainingArgs: Seq[String]
  ): Either[EarlyReturn, Unit] = {
    Either.cond(
      remainingArgs.isEmpty,
      (),
      ParsingError(
        Error.Other(s"Found extra arguments: ${remainingArgs.mkString(" ")}")
      )
    )
  }

  /**
    * Handle the result of a sub-command parse.
    */
  private def commandMain(
    commonOpts: CommonOptions,
    commandParseWithArgs: (String, WithHelp[ClioCommand], RemainingArgs)
  ): Either[EarlyReturn, Future[HttpResponse]] = {
    val (commandName, commandParse, args) = commandParseWithArgs

    for {
      _ <- messageIfAsked(commandHelp(commandName), commandParse.help)
      _ <- messageIfAsked(commandUsage(commandName), commandParse.usage)
      token <- getAccessToken(commonOpts.bearerToken)
      command <- wrapError(commandParse.baseOrError)
      _ <- checkRemainingArgs(args.remaining)
    } yield {
      implicit val bearerToken: OAuth2BearerToken = token
      val commandDispatch = new CommandDispatch(webClient, ioUtil)
      commandDispatch.dispatch(command)
    }
  }

}
