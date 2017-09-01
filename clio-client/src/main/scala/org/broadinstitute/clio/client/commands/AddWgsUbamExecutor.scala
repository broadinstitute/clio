package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.parser.parse
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferWgsUbamV1Metadata

import scala.concurrent.{ExecutionContext, Future}
class AddWgsUbamExecutor(addWgsUbam: AddWgsUbam) extends Executor {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    val metadataLoc = addWgsUbam.metadataLocation

    val parsedOrError = parse(IoUtil.readMetadata(metadataLoc)).left.map {
      err =>
        new RuntimeException(
          s"Could not parse contents of $metadataLoc as JSON.",
          err
        )
    }

    val decodedOrError = parsedOrError
      .flatMap(_.as[TransferWgsUbamV1Metadata])
      .left
      .map { err =>
        new RuntimeException(
          s"Invalid metadata given at $metadataLoc. Run the `get-wgs-ubam-schema` command to see the expected JSON format for WgsUbams.",
          err
        )
      }

    decodedOrError.fold(
      Future
        .failed(_) logErrorMsg s"Metadata at $metadataLoc cannot be added to Clio", {
        decoded =>
          webClient.addWgsUbam(
            transferWgsUbamV1Metadata = decoded,
            input = addWgsUbam.transferWgsUbamV1Key
          )
      }
    )
  }
}
