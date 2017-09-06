package org.broadinstitute.clio.client.webclient

import org.broadinstitute.clio.client.util.{IoUtil, TestData}
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.status.model.{
  ServerStatusInfo,
  StatusInfo,
  SystemStatusInfo,
  VersionInfo
}
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput
}
import org.broadinstitute.clio.util.json.JsonSchemas

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.{Json, Printer}
import io.circe.parser.parse
import io.circe.syntax._

import scala.concurrent.Future

class MockClioWebClient(
  status: StatusCode,
  metadataLocationOption: Option[String]
)(implicit system: ActorSystem)
    extends ClioWebClient(
      "localhost",
      MockClioWebClient.testServerPort,
      false,
      MockClioWebClient.testRequestTimeout
    )
    with TestData {

  val health = StatusInfo(ServerStatusInfo.Started, SystemStatusInfo.OK)
  val version = VersionInfo("0.0.1")

  val json: Option[Json] = metadataLocationOption.map(
    metadataLocation =>
      parse(IoUtil.readMetadata(metadataLocation)) match {
        case Right(value) => value
        case Left(parsingFailure) =>
          throw parsingFailure
    }
  )

  override def getClioServerHealth: Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          health.asJson.pretty(implicitly[Printer])
        )
      )
    )
  }

  override def getClioServerVersion: Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          version.asJson.pretty(implicitly[Printer])
        )
      )
    )
  }

  override def getWgsUbamSchema(
    implicit bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          JsonSchemas.WgsUbam.pretty(implicitly[Printer])
        )
      )
    )
  }

  override def addWgsUbam(
    input: TransferWgsUbamV1Key,
    transferWgsUbamV1Metadata: TransferWgsUbamV1Metadata
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    Future.successful(HttpResponse(status = status))
  }

  override def queryWgsUbam(
    input: TransferWgsUbamV1QueryInput,
    includeDeleted: Boolean = false
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          json
            .map(_.pretty(implicitly))
            .getOrElse(Json.arr().pretty(implicitly))
        )
      )
    )
  }
}

object MockClioWebClient extends TestData {
  def returningOk(implicit system: ActorSystem) =
    new MockClioWebClient(status = StatusCodes.OK, None)

  def returningInternalError(implicit system: ActorSystem) =
    new MockClioWebClient(
      status = StatusCodes.InternalServerError,
      testWgsUbamLocation
    )

  def returningWgsUbam(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, testWgsUbamLocation)
  }

  def returningTwoWgsUbams(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, testTwoWgsUbamsLocation)
  }

  def returningNoWgsUbam(implicit system: ActorSystem): MockClioWebClient = {
    class MockClioWebClientNoReturn
        extends MockClioWebClient(status = StatusCodes.OK, None) {
      override def queryWgsUbam(
        input: TransferWgsUbamV1QueryInput,
        includeDeleted: Boolean = false
      )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
        Future.successful(
          HttpResponse(
            status = StatusCodes.OK,
            entity = HttpEntity(
              ContentTypes.`application/json`,
              Json.arr().pretty(implicitly)
            )
          )
        )
      }
    }
    new MockClioWebClientNoReturn
  }

  def failingToAddWgsUbam(implicit system: ActorSystem): MockClioWebClient = {
    class MockClioWebClientCantAdd
        extends MockClioWebClient(status = StatusCodes.OK, testWgsUbamLocation) {
      override def addWgsUbam(
        input: TransferWgsUbamV1Key,
        transferWgsUbamV1Metadata: TransferWgsUbamV1Metadata
      )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
        Future.successful(
          HttpResponse(status = StatusCodes.InternalServerError)
        )
      }
    }
    new MockClioWebClientCantAdd
  }
}
