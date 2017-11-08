package org.broadinstitute.clio.client.dispatch

import io.circe.parser.parse
import org.broadinstitute.clio.client.commands.{AddCommand, ClioCommand}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.{ExecutionContext, Future}

class AddExecutor[TI <: TransferIndex](addCommand: AddCommand[TI])
    extends Executor[UpsertId] {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[UpsertId] = {

    import addCommand.index.implicits._

    val location = addCommand.metadataLocation
    val commandName = addCommand.index.commandName
    val name = addCommand.index.name

    val parsedOrError = parse(IoUtil.readMetadata(location)).left.map { err =>
      new RuntimeException(
        s"Could not parse contents of $location as JSON.",
        err
      )
    }

    val decodedOrError = parsedOrError
      .flatMap(_.as[addCommand.index.MetadataType])
      .left
      .map { err =>
        new RuntimeException(
          s"Invalid metadata given at $location. Run the '${ClioCommand.getSchemaPrefix}$commandName' command to see the expected JSON format for ${name}s.",
          err
        )
      }

    decodedOrError.fold(
      Future
        .failed(_) logErrorMsg s"Metadata at $location cannot be added to Clio", {
        decoded =>
          webClient.upsert(addCommand.index)(addCommand.key, decoded)
      }
    )
  }
}
