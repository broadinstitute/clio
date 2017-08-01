package org.broadinstitute.clio.client.commands

import io.circe.parser._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferReadGroupLocation,
  TransferReadGroupV1Key,
  TransferReadGroupV1Metadata
}

class AddReadGroupBam(clioWebClient: ClioWebClient,
                      flowcell: String,
                      lane: Int,
                      libraryName: String,
                      location: String,
                      metadataLocation: String,
                      bearerToken: String)
    extends Command(clioWebClient = clioWebClient) {

  override def execute: Boolean = {
    //parse metadata to validate inputs
    val json = parse(IoUtil.readMetadata(metadataLocation)) match {
      case Right(value) => value
      case Left(parsingFailure) =>
        clioWebClient.shutdown()
        throw parsingFailure
    }

    val decoded = json.as[TransferReadGroupV1Metadata] match {
      case Right(value) => value
      case Left(decodingFailure) =>
        clioWebClient.shutdown()
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
          location = TransferReadGroupLocation.pathMatcher(location)
        )
      )

    checkResponseAndShutdown(Commands.addReadGroupBam, responseFuture)
  }

}
