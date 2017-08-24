package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.circe.Json
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object MoveWgsUbamCommand extends Command with LazyLogging {

  implicit val implicitLogger: Logger = logger

  implicit class FutureErrorMessage[A](future: Future[A]) {
    def withErrorMsg(message: String)(implicit ec: ExecutionContext,
                                      logger: Logger): Future[A] = {
      future andThen {
        case Success(a) => Future.successful(a)
        case Failure(ex) =>
          logger.error(message)
          Future.failed(new Exception(message, ex))
      }
    }
  }

  override def execute(webClient: ClioWebClient, config: BaseArgs)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    for {
      wgsUbamsResponse <- queryForWgsUbam(webClient, config) withErrorMsg
        "Could not query the WgsUbam. No files have been moved"
      firstJson <- getFirstObject(wgsUbamsResponse) withErrorMsg
        "Could not get only 1 WgsUbam from Clio. No files have been moved"
      moveWgsUbam <- moveGoogleObject(
        firstJson.asObject.get("ubamPath").get.asString,
        config.ubamPath
      ) withErrorMsg
        "An error occurred while moving the files in google cloud. No files have been moved"
      upsertUbam <- upsertUpdatedWgsUbam(webClient, config) withErrorMsg
        """An error occurred while upserting the WgsUbam. The state of clio is now inconsistent.
          |Please verify that the unmapped bam was moved in google cloud.
          |Then, try upserting (not moving) the unmapped bam with the correct path.
          |If the state of Clio cannot be fixed, please contact the Green Team at greenteam@broadinstitute.org
        """.stripMargin
    } yield {
      upsertUbam
    }
  }

  private def queryForWgsUbam(webClient: ClioWebClient,
                              config: BaseArgs): Future[Json] = {
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
    webClient.getResponseAsJson(httpResponse)
  }

  private def moveGoogleObject(source: Option[String],
                               destination: Option[String]): Future[Unit] = {
    IoUtil.moveGoogleObject(source.get, destination.get) match {
      case 0 => Future.successful(())
      case _ =>
        Future.failed(new Exception("Copy files in google cloud failed"))
    }
  }

  private def getFirstObject(ubamsResponseJson: Json): Future[Json] = {
    ubamsResponseJson.asArray.size match {
      case 1 => Future.successful(ubamsResponseJson.asArray.get.head)
      case _ =>
        Future.failed(
          new IllegalStateException(
            "The number of unmapped bams returned was not equal to 1"
          )
        )
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
