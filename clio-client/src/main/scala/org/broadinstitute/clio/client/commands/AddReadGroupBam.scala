package org.broadinstitute.clio.client.commands

import io.circe.parser._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferReadGroupV1Key,
  TransferReadGroupV1Metadata
}
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class AddReadGroupBam(clioWebClient: ClioWebClient,
                      flowcell: String,
                      lane: Int,
                      libraryName: String,
                      location: String,
                      metadataLocation: String,
                      bearerToken: String)(implicit ec: ExecutionContext)
    extends Command(Commands.addReadGroupBam) {

  override def execute: Boolean = {
    //parse metadata to validate inputs
    val json = parse(IoUtil.readMetadata(metadataLocation)) match {
      case Right(value) => value
      case Left(parsingFailure) =>
        throw parsingFailure
    }

    val decoded = json.as[TransferReadGroupV1Metadata] match {
      case Right(value) => value
      case Left(decodingFailure) =>
        throw decodingFailure
    }

    val responseFuture =
      clioWebClient.addReadGroupBam(
        bearerToken = bearerToken,
        transferReadGroupV1Metadata = decoded,
        input = TransferReadGroupV1Key(
          flowcellBarcode = flowcell,
          lane = lane,
          libraryName = libraryName,
          location = Location.pathMatcher(location)
        )
      )

    checkResponse(responseFuture)
  }

}
