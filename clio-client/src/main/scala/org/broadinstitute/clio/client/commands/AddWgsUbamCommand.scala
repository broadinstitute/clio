package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata
}
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.{ExecutionContext, Future}
import io.circe.parser._
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._

object AddWgsUbamCommand extends Command {
  def execute(webClient: ClioWebClient, config: BaseArgs)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    val decodedOrError = parse(IoUtil.readMetadata(config.metadataLocation.get))
      .flatMap(_.as[TransferWgsUbamV1Metadata])

    decodedOrError.fold(
      Future.failed, { decoded =>
        webClient.addWgsUbam(
          bearerToken = config.bearerToken.getOrElse(""),
          transferWgsUbamV1Metadata = decoded,
          input = TransferWgsUbamV1Key(
            flowcellBarcode = config.flowcell.get,
            lane = config.lane.get,
            libraryName = config.libraryName.get,
            location = Location.pathMatcher(config.location.get)
          )
        )
      }
    )
  }
}
