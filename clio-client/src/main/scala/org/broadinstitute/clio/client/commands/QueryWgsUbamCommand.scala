package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Json
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferWgsUbamV1QueryInput
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object QueryWgsUbamCommand
    extends Command
    with LazyLogging
    with FailFastCirceSupport
    with ModelAutoDerivation {
  def execute(webClient: ClioWebClient, config: BaseArgs, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    webClient
      .queryWgsUbam(
        bearerToken = config.bearerToken.getOrElse(""),
        TransferWgsUbamV1QueryInput(
          flowcellBarcode = config.flowcell,
          lane = config.lane,
          libraryName = config.libraryName,
          location = config.location.map(s => Location.pathMatcher(s)),
          lcSet = config.lcSet,
          project = config.project,
          sampleAlias = config.sampleAlias,
          runDateEnd = config.runDateEnd,
          runDateStart = config.runDateStart,
          documentStatus = config.documentStatus
        )
      )
      .andThen {
        case Success(response) =>
          webClient.unmarshal[Json](response).andThen {
            case Success(json) => logger.info(json.toString())
            case Failure(ex)   => logger.error(ex.getMessage)
          }
      }
  }
}
