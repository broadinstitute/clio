package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{TransferWgsUbamV1Key, TransferWgsUbamV1Metadata, TransferWgsUbamV1QueryInput, TransferWgsUbamV1QueryOutput}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.{ExecutionContext, Future}

object MoveWgsUbamCommand extends Command {

  override def execute(webClient: ClioWebClient, config: BaseArgs)
                      (implicit ec: ExecutionContext): Future[HttpResponse] = {
    for {
      wgsUbams <- webClient.getWgsUbamModels(config.bearerToken.getOrElse(""),
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
      _ <- assertOnlyOne(wgsUbams)
      moveWgsUbam <- IoUtil.moveGoogleObject(wgsUbams.head.ubamPath.get, config.ubamPath.get)
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

  private def assertOnlyOne(ubams: Seq[TransferWgsUbamV1QueryOutput]): Future[Unit] = {
    ubams.size match {
      case 1 => Future.successful(())
      case _ => Future.failed(new IllegalStateException("The number of unmapped bams returned did not equal 1"))
    }
  }
}
