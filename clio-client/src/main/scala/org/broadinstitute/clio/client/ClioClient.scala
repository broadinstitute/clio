package org.broadinstitute.clio.client

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.{Done, NotUsed}
import caseapp.core.help.{Help, WithHelp}
import caseapp.core.parser.Parser
import caseapp.core.{Error, RemainingArgs}
import cats.syntax.either._
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.dispatch._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.auth.ClioCredentials

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

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
  final case class UsageOrHelpAsked(message: String) extends EarlyReturn

  val progName = "clio-client"

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem(progName)
    implicit val mat: Materializer = ActorMaterializer()

    import system.dispatcher
    sys.addShutdownHook({ val _ = system.terminate() })

    val baseCreds = Either
      .catchNonFatal(new ClioCredentials(ClioClientConfig.serviceAccountJson))
      .valueOr { err =>
        logger.error("Failed to read credentials", err)
        sys.exit(1)
      }

    def early(source: ClioClient.EarlyReturn): Unit = {
      source match {
        case UsageOrHelpAsked(message) => {
          println(message)
          sys.exit(0)
        }
        case ParsingError(error) => {
          System.err.println(error.message)
          sys.exit(1)
        }
      }
    }

    def complete(done: Try[Done]): Unit = {
      val status = done match {
        case Success(_) => 0
        case Failure(ex) =>
          logger.error("Failed to execute command", ex)
          1
      }
      Await.result(system.terminate(), 23.seconds)
      sys.exit(status)
    }

    def otherwise(source: Source[Json, NotUsed]): Unit = {
      source
        .runWith(Sink.ignore)
        .onComplete(complete)
    }

    new ClioClient(ClioWebClient(baseCreds), IoUtil(baseCreds))
      .instanceMain(args)
      .fold(early, otherwise)
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
  */
class ClioClient(webClient: ClioWebClient, ioUtil: IoUtil)(
  implicit ec: ExecutionContext
) {
  import ClioClient.{EarlyReturn, ParsingError, UsageOrHelpAsked}

  /**
    * Common option messages, updated to include our program
    * name and version info.
    *
    * caseapp supports setting these through annotations, but
    * only if the values are constant strings.
    */
  private val beforeCommandHelp =
    Help(
      appName = "Clio Client",
      appVersion = ClioClientConfig.Version.value,
      progName = ClioClient.progName,
      optionsDesc = "[command] [command-options]",
      args = Seq.empty,
      argsNameOption = None
    )

  /** Names of all valid sub-commands. */
  private val commands: Seq[String] =
    ClioCommand.help.messagesMap.keys.toSeq.sorted

  /** Top-level help message to display on --help. */
  private val helpMessage: String =
    s"""${beforeCommandHelp.help.stripLineEnd}
       |Available commands:
       |${commands.map(commandHelp).mkString("\n\n")}
     """.stripMargin

  /**
    * Top-level usage message to display on --usage,
    * or when users give no sub-command.
    */
  private val usageMessage: String =
    s"""${beforeCommandHelp.usage}
       |Available commands:
       |  ${commands.mkString(", ")}
     """.stripMargin

  /** Help message for a specific command. */
  private def commandHelp(command: String): String = {
    ClioCommand.help
      .messagesMap(command)
      .helpMessage(s"${beforeCommandHelp.progName} [options]", command)
      .trim()
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
    args: Array[String],
    print: Any => Unit = Predef.print
  ): Either[EarlyReturn, Source[Json, NotUsed]] = {
    val maybeParse =
      ClioCommand.parser.withHelp
        .detailedParse(args)(Parser[None.type].withHelp)

    wrapError(maybeParse).flatMap {
      case (commonParse, commonArgs, maybeCommandParse) => {
        for {
          _ <- messageIfAsked(helpMessage, commonParse.help)
          _ <- messageIfAsked(usageMessage, commonParse.usage)
          _ <- wrapError(commonParse.baseOrError)
          _ <- checkRemainingArgs(commonArgs)
          response <- maybeCommandParse.map { commandParse =>
            wrapError(commandParse)
              .flatMap(commandParse => commandMain(commandParse, print))
          }.getOrElse(Left(ParsingError(Error.Other(usageMessage))))
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
    either.leftMap(ParsingError.apply)
  }

  /**
    * Utility for wrapping a help / usage message in our type
    * infrastructure, for cleaner use in for-comprehensions.
    */
  private def messageIfAsked(
    message: String,
    asked: Boolean
  ): Either[EarlyReturn, Unit] = {
    Either.cond(!asked, (), UsageOrHelpAsked(message))
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
    commandParseWithArgs: (String, WithHelp[ClioCommand], RemainingArgs),
    print: Any => Unit
  ): Either[EarlyReturn, Source[Json, NotUsed]] = {
    val (commandName, commandParse, args) = commandParseWithArgs

    for {
      _ <- messageIfAsked(commandHelp(commandName), commandParse.help)
      _ <- messageIfAsked(commandUsage(commandName), commandParse.usage)
      command <- wrapError(commandParse.baseOrError)
      _ <- checkRemainingArgs(args.remaining)
    } yield {
      val executor = command match {
        case deliverCommand: DeliverCommand[_] => new DeliverExecutor(deliverCommand)
        case addCommand: AddCommand[_]         => new AddExecutor(addCommand)
        case moveCommand: MoveCommand[_]       => new MoveExecutor(moveCommand)
        case deleteCommand: DeleteCommand[_]   => new DeleteExecutor(deleteCommand)
        case patchCommand: PatchCommand[_]     => new PatchExecutor(patchCommand)
        case markExternalCommand: MarkExternalCommand[_] =>
          new MarkExternalExecutor(markExternalCommand)
        case retrieveAndPrint: RetrieveAndPrintCommand =>
          new RetrieveAndPrintExecutor(retrieveAndPrint, print)
      }

      executor.execute(webClient, ioUtil)
    }
  }
}
