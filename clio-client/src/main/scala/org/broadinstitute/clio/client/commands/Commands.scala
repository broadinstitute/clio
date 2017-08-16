package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser._
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferReadGroupV1Key,
  TransferReadGroupV1Metadata,
  TransferReadGroupV1QueryInput
}
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.{Await, ExecutionContext, Future}

object Commands {
  sealed abstract class Command(
    val commandName: String
  )(implicit ec: ExecutionContext)
      extends LazyLogging {
    def execute(clioWebClient: ClioWebClient, config: BaseArgs): Boolean

    def checkResponse(responseFuture: Future[HttpResponse]): Boolean = {
      Await.result(
        responseFuture.map[Boolean] { response =>
          val isSuccess = response.status.isSuccess()

          if (isSuccess) {
            logger.info(
              s"Successfully completed command $commandName." +
                s" Response code: ${response.status}"
            )
          } else {
            logger.error(
              s"Error executing command $commandName." +
                s" Response code: ${response.status}"
            )
          }
          isSuccess
        },
        ClioClientConfig.responseTimeout
      )
    }
  }
  case class AddReadGroupBam()(implicit ec: ExecutionContext)
      extends Command(commandName = "addReadGroupBam") {
    override def execute(clioWebClient: ClioWebClient,
                         config: BaseArgs): Boolean = {
      //parse metadata to validate inputs
      val json = parse(IoUtil.readMetadata(config.metadataLocation)) match {
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
          bearerToken = config.bearerToken,
          transferReadGroupV1Metadata = decoded,
          input = TransferReadGroupV1Key(
            flowcellBarcode = config.flowcell.get,
            lane = config.lane.get,
            libraryName = config.libraryName.get,
            location = Location.pathMatcher(config.location.get)
          )
        )

      checkResponse(responseFuture)
    }
  }

  case class QueryReadGroupBam()(implicit ec: ExecutionContext)
      extends Command(commandName = "queryReadGroupBam") {
    override def execute(clioWebClient: ClioWebClient,
                         config: BaseArgs): Boolean = {

      val responseFuture = clioWebClient.queryReadGroupBam(
        bearerToken = config.bearerToken,
        TransferReadGroupV1QueryInput(
          flowcellBarcode = config.flowcell,
          lane = config.lane,
          libraryName = config.libraryName,
          location = config.location.map(s => Location.pathMatcher(s)),
          lcSet = config.lcSet,
          project = config.project,
          sampleAlias = config.sampleAlias,
          runDateEnd = config.runDateEnd,
          runDateStart = config.runDateStart
        )
      )

      checkResponse(responseFuture)
    }
  }
}
