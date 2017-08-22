package org.broadinstitute.clio.client.commands

import io.circe.parser._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata
}
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class AddWgsUbam(clioWebClient: ClioWebClient,
                 flowcell: String,
                 lane: Int,
                 libraryName: String,
                 location: String,
                 metadataLocation: String,
                 bearerToken: String)(implicit ec: ExecutionContext)
    extends Command(Commands.addWgsUbam) {

  override def execute: Boolean = {
    //parse metadata to validate inputs
    val json = parse(IoUtil.readMetadata(metadataLocation)) match {
      case Right(value) => value
      case Left(parsingFailure) =>
        throw parsingFailure
    }

    val decoded = json.as[TransferWgsUbamV1Metadata] match {
      case Right(value) => value
      case Left(decodingFailure) =>
        throw decodingFailure
    }

    val responseFuture =
      clioWebClient.addWgsUbam(
        bearerToken = bearerToken,
        transferWgsUbamV1Metadata = decoded,
        input = TransferWgsUbamV1Key(
          flowcellBarcode = flowcell,
          lane = lane,
          libraryName = libraryName,
          location = Location.pathMatcher(location)
        )
      )

    checkResponse(responseFuture)
  }

}
