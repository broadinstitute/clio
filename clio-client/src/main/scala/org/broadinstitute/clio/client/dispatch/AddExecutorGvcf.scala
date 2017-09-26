package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpCredentials
import io.circe.parser.parse
import org.broadinstitute.clio.client.commands.{AddGvcf, ClioCommand}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  GvcfIndex,
  TransferGvcfV1Metadata
}

import scala.concurrent.{ExecutionContext, Future}

class AddExecutorGvcf(addGvcf: AddGvcf) extends Executor {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {
    val metadataLoc = addGvcf.metadataLocation

    val parsedOrError = parse(IoUtil.readMetadata(metadataLoc)).left.map {
      err =>
        new RuntimeException(
          s"Could not parse contents of $metadataLoc as JSON.",
          err
        )
    }

    val decodedOrError = parsedOrError
      .flatMap(_.as[TransferGvcfV1Metadata])
      .left
      .map { err =>
        new RuntimeException(
          s"Invalid metadata given at $metadataLoc. Run the ${ClioCommand.getGvcfSchemaName} command to see the expected JSON format for Gvcfs.",
          err
        )
      }

    decodedOrError.fold(
      Future
        .failed(_) logErrorMsg s"Metadata at $metadataLoc cannot be added to Clio", {
        decoded =>
          webClient.upsert(GvcfIndex, addGvcf.transferGvcfV1Key, decoded)
      }
    )
  }
}
