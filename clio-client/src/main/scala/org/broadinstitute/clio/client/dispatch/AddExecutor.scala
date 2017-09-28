package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpCredentials
import io.circe.{Decoder, Encoder}
import io.circe.parser.parse
import org.broadinstitute.clio.client.commands.{AddCommand, ClioCommand}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}

class AddExecutor(addCommand: AddCommand) extends Executor {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {

    val location = addCommand.metadataLocation
    val index = addCommand.index

    implicit val decoder: Decoder[index.MetadataType] = index.metadataDecoder
    implicit val encoder: Encoder[index.MetadataType] = index.metadataEncoder

    val parsedOrError = parse(IoUtil.readMetadata(location)).left.map { err =>
      new RuntimeException(
        s"Could not parse contents of $location as JSON.",
        err
      )
    }

    val decodedOrError = parsedOrError
      .flatMap(_.as[index.MetadataType])
      .left
      .map { err =>
        new RuntimeException(
          s"Invalid metadata given at $location. Run the '${ClioCommand.getSchemaPrefix}${index.commandName}' command to see the expected JSON format for ${index.name}s.",
          err
        )
      }

    decodedOrError.fold(
      Future
        .failed(_) logErrorMsg s"Metadata at $location cannot be added to Clio", {
        decoded =>
          webClient.upsert(index, addCommand.key, decoded)
      }
    )
  }
}
