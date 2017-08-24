package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import io.circe.Json
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{TransferWgsUbamV1Key, TransferWgsUbamV1Metadata, TransferWgsUbamV1QueryInput}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}
import io.circe.parser.parse

import scala.concurrent.{ExecutionContext, Future}

object MoveWgsUbamCommand extends Command {

  override def execute(webClient: ClioWebClient, config: BaseArgs)
                      (implicit ec: ExecutionContext): Future[HttpResponse] = {
    for {
      wgsUbamsResponse <- webClient.queryWgsUbam(config.bearerToken.getOrElse(""),
        TransferWgsUbamV1QueryInput(
          flowcellBarcode = config.flowcell,
          lane = config.lane,
          libraryName = config.libraryName,
          location = Option(Location.GCP),
          lcSet = None,
          project = None,
          sampleAlias = None,
          runDateEnd = None,
          runDateStart = None,
          documentStatus = Option(DocumentStatus.Normal)
        ))
      firstJson <- assertOnlyOne(wgsUbamsResponse, webClient)
      moveWgsUbam <- IoUtil.moveGoogleObject(firstJson.asObject.get("ubamPath").get.asString.get, config.ubamPath.get)
      upsertUbam <- webClient.addWgsUbam(
        bearerToken = config.bearerToken.getOrElse(""),
        input = TransferWgsUbamV1Key(
          flowcellBarcode = config.flowcell.get,
          lane = config.lane.get,
          libraryName = config.libraryName.get,
          location = Location.GCP
        ),
        transferWgsUbamV1Metadata = TransferWgsUbamV1Metadata(
          ubamPath = config.ubamPath
        )
      )
    } yield {
      upsertUbam
    }
  }

  private def assertOnlyOne(ubamsResponse: HttpResponse, webClient: ClioWebClient): Future[Json] = {
    implicit val mat: ActorMaterializer = webClient.materializer
    implicit val ec: ExecutionContext = webClient.executionContext
    Unmarshal(ubamsResponse).to[String].flatMap( s => {
      parse(s) match {
        case Left(f) => Future.failed(f)
        case Right(json) =>
          json.asArray.size match {
            case 1 => Future.successful(json.asArray.get.head)
            case _ => Future.failed(new IllegalStateException("The number of unmapped bams returned was not equal to 1"))
          }
      }
    })
  }
}
