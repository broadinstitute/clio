package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.{LazyLogging, Logger}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.{ExecutionContext, Future}
import org.broadinstitute.clio.client.util.FutureWithErrorMessage.FutureWithErrorMessage
import org.broadinstitute.clio.util.json.ModelAutoDerivation

object MoveWgsUbamCommand
    extends Command
    with LazyLogging
    with FailFastCirceSupport
    with ModelAutoDerivation {

  implicit val implicitLogger: Logger = logger

  override def execute(
    webClient: ClioWebClient,
    config: BaseArgs
  )(implicit ec: ExecutionContext, ioUtil: IoUtil): Future[HttpResponse] = {
    for {
      wgsUbam <- queryForWgsUbam(webClient, config) withErrorMsg
        """Could not query the WgsUbam. There may have been the incorrect number of WgsUbams returned.
          |No files have been moved""".stripMargin
      _ <- copyGoogleObject(wgsUbam.ubamPath, config.ubamPath) withErrorMsg
        "An error occurred while moving the files in google cloud. No files have been moved"
      upsertUbam <- upsertUpdatedWgsUbam(webClient, config) withErrorMsg
        """An error occurred while upserting the WgsUbam. The state of clio is now inconsistent.
          |The ubam exists in both at both the old and the new locations.
          |At this time, Clio only knows about the bam at the old location.
          |Then, try upserting (not moving) the unmapped bam with the correct path then deleting the old bam.
          |If the state of Clio cannot be fixed, please contact the Green Team at greenteam@broadinstitute.org
        """.stripMargin
      _ <- deleteGoogleObject(wgsUbam.ubamPath) withErrorMsg
        """The old bam was not able to be deleted. Clio has been updated to point to the new bam.
          | Please delete the old bam. If this cannot be done, contact Green Team at greenteam@broadinstitute.org
          | """.stripMargin
    } yield {
      upsertUbam
    }
  }

  private def queryForWgsUbam(
    webClient: ClioWebClient,
    config: BaseArgs
  ): Future[TransferWgsUbamV1QueryOutput] = {
    val httpResponse = webClient.queryWgsUbam(
      config.bearerToken.getOrElse(""),
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
      )
    )
    implicit val ec: ExecutionContext = webClient.executionContext
    webClient
      .unmarshal[Seq[TransferWgsUbamV1QueryOutput]](httpResponse)
      .map(seq => {
        seq.size match {
          case 1 => seq.head
          case s if s > 1 =>
            throw new Exception("There was more than one WgsUbam returned")
          case 0 => throw new Exception("No WgsUbams were returned")
        }
      })
  }

  private def copyGoogleObject(
    source: Option[String],
    destination: Option[String]
  )(implicit ioUtil: IoUtil): Future[Unit] = {
    ioUtil.copyGoogleObject(source.get, destination.get) match {
      case 0 => Future.successful(())
      case _ =>
        Future.failed(new Exception("Copy files in google cloud failed"))
    }
  }

  private def deleteGoogleObject(
    path: Option[String]
  )(implicit ioUtil: IoUtil): Future[Unit] = {
    ioUtil.deleteGoogleObject(path.get) match {
      case 0 => Future.successful(())
      case _ =>
        Future.failed(new Exception("Delete files in google cloud failed"))
    }
  }

  private def upsertUpdatedWgsUbam(webClient: ClioWebClient,
                                   config: BaseArgs): Future[HttpResponse] = {
    val addResponse = webClient.addWgsUbam(
      bearerToken = config.bearerToken.getOrElse(""),
      input = TransferWgsUbamV1Key(
        flowcellBarcode = config.flowcell.get,
        lane = config.lane.get,
        libraryName = config.libraryName.get,
        location = Location.GCP
      ),
      transferWgsUbamV1Metadata =
        TransferWgsUbamV1Metadata(ubamPath = config.ubamPath)
    )
    webClient.ensureOkResponse(addResponse)
  }
}
