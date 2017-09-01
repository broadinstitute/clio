package org.broadinstitute.clio.client

import org.broadinstitute.clio.client.commands.{ClioCommand, CommonOptions}
import org.broadinstitute.clio.client.dispatch.CommandDispatch
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import caseapp.core.{Messages, WithHelp}
import com.typesafe.scalalogging.LazyLogging

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
object ClioClient {

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
  final case class ParsingError(error: String) extends EarlyReturn
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
            Future.successful(())
          }
          case ParsingError(error) => {
            System.err.println(error)
            Future.failed(new RuntimeException)
          }
        },
        identity
      )
      .onComplete {
        case Success(_) => sys.exit(0)
        case Failure(_) => sys.exit(1)
      }
  }
}

/**
  * The main clio-client application.
  *
  * Inspired by / partially copy-pasted from the
  * [[caseapp.CommandAppWithPreCommand]], which demonstrates
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
  import ClioClient.{EarlyReturn, ParsingError, UsageOrHelpAsked}

  val progName: String = ClioClient.progName
  val appName: String = "Clio Client"
  val appVersion: String = ClioClientConfig.Version.value

  /**
    * Common option messages, updated to include our program
    * name and version info.
    *
    * caseapp supports setting these through annotations, but
    * only if the values are constant strings.
    */
  private val beforeCommandMessages: Messages[CommonOptions] =
    CommonOptions.messages.copy(
      appName = appName,
      appVersion = appVersion,
      progName = progName,
      optionsDesc = s"[options] [command] [command-options]"
    )

  /** Names of all valid sub-commands. */
  private val commands: Seq[String] =
    ClioCommand.messages.messages.map(_._1)

  /** Top-level help message to display on --help. */
  private val helpMessage: String =
    s"""
       |${beforeCommandMessages.helpMessage}
       |Available commands:
       |${commands.map(commandHelp).mkString("\n")}
     """.stripMargin

  /**
    * Top-level usage message to display on --usage,
    * or when users give no sub-command.
    */
  private val usageMessage: String =
    s"""
       |${beforeCommandMessages.usageMessage}
       |Available commands:
       |  ${commands.mkString(", ")}
     """.stripMargin

  /** Help message for a specific command. */
  private def commandHelp(command: String): String = {
    ClioCommand.messages
      .messagesMap(command)
      .helpMessage(s"${beforeCommandMessages.progName} [options]", command)
  }

  /** Usage message for a specific command. */
  private def commandUsage(command: String): String = {
    ClioCommand.messages
      .messagesMap(command)
      .usageMessage(
        s"${beforeCommandMessages.progName} [options]",
        s"$command [command-options]"
      )
  }

  /**
    * The client's entry point.
    *
    * Note: this can't be named `main` because if it is, the
    * compiler will try to be helpful and generate a forwarder
    * method also named `main` in the companion object that will
    * do nothing but call this method (allowing this class to serve
    * as a main entry-point), conflicting with the main method
    * already present in the companion object.
    */
  def instanceMain(
    args: Array[String]
  ): Either[EarlyReturn, Future[HttpResponse]] = {
    val maybeParse =
      ClioCommand.parser.withHelp
        .detailedParse(args)(CommonOptions.parser.withHelp)
        .left
        .map(ParsingError.apply)

    maybeParse.flatMap {
      case (commonParse, commonArgs, maybeCommandParse) => {
        for {
          _ <- messageIfAsked(helpMessage, commonParse.help)
          _ <- messageIfAsked(usageMessage, commonParse.usage)
          commonOpts <- commonParse.baseOrError.left.map(ParsingError.apply)
          _ <- checkRemainingArgs(commonArgs)
          response <- maybeCommandParse
            .map { commandParse =>
              commandParse.left
                .map(ParsingError.apply)
                .flatMap(commandMain(commonOpts, _))
            }
            .getOrElse(Left(ParsingError(usageMessage)))
        } yield {
          response
        }
      }
    }
  }

  /**
    * Utility for wrapping a help / usage message in our type
    * infrastructure, for cleaner use in for-comprehensions.
    */
  private def messageIfAsked(message: String,
                             asked: Boolean): Either[EarlyReturn, Unit] = {
    if (asked) {
      Left(UsageOrHelpAsked(message))
    } else {
      Right(())
    }
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
      ParsingError(s"Found extra arguments: ${remainingArgs.mkString(" ")}")
    )
  }

  /**
    * Handle the result of a sub-command parse.
    */
  private def commandMain(
    commonOpts: CommonOptions,
    commandParseWithArgs: (String,
                           WithHelp[ClioCommand],
                           Seq[String],
                           Seq[String])
  ): Either[EarlyReturn, Future[HttpResponse]] = {
    val (commandName, commandParse, args, args0) = commandParseWithArgs

    for {
      _ <- messageIfAsked(commandHelp(commandName), commandParse.help)
      _ <- messageIfAsked(commandUsage(commandName), commandParse.usage)
      command <- commandParse.baseOrError.left.map(ParsingError.apply)
      _ <- checkRemainingArgs(args ++ args0)
    } yield {
      implicit val bearerToken: OAuth2BearerToken = commonOpts.bearerToken
      val commandDispatch = new CommandDispatch(webClient, ioUtil)
      commandDispatch.dispatch(command)
    }
  }

}
