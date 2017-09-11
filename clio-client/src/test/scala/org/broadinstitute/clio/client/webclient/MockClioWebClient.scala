package org.broadinstitute.clio.client.webclient

import org.broadinstitute.clio.client.util.{IoUtil, TestData}
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.JsonSchemas

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.{Json, Printer}
import io.circe.parser.parse

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

  val version: String =
    """|{
       |  "version" : "0.0.1"
       |}""".stripMargin

  val json: Option[Json] = metadataLocationOption.map(
    metadataLocation =>
      parse(IoUtil.readMetadata(metadataLocation)) match {
        case Right(value) => value
        case Left(parsingFailure) =>
          throw parsingFailure
    }
  )

  override def getClioServerVersion: Future[HttpResponse] = {
    Future.successful(
      HttpResponse(status = status, entity = HttpEntity(version))
    )
  }

  override def getSchemaWgsUbam(
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

  override def getSchemaGvcf(
    implicit bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          JsonSchemas.Gvcf.pretty(implicitly[Printer])
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

  override def addGvcf(
    input: TransferGvcfV1Key,
    transferGvcfV1Metadata: TransferGvcfV1Metadata
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    Future.successful(HttpResponse(status = status))
  }

  override def queryWgsUbam(
    input: TransferWgsUbamV1QueryInput
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

  override def queryGvcf(
    input: TransferGvcfV1QueryInput
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

  def returningInternalErrorWgsUbam(implicit system: ActorSystem) =
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
        input: TransferWgsUbamV1QueryInput
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

  def returningInternalErrorGvcf(implicit system: ActorSystem) =
    new MockClioWebClient(
      status = StatusCodes.InternalServerError,
      testGvcfLocation
    )

  def returningGvcf(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, testGvcfLocation)
  }

  def returningTwoGvcfs(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, testTwoGvcfsLocation)
  }

  def returningNoGvcf(implicit system: ActorSystem): MockClioWebClient = {
    class MockClioWebClientNoReturn
        extends MockClioWebClient(status = StatusCodes.OK, None) {
      override def queryGvcf(
        input: TransferGvcfV1QueryInput
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

  def failingToAddGvcf(implicit system: ActorSystem): MockClioWebClient = {
    class MockClioWebClientCantAdd
        extends MockClioWebClient(status = StatusCodes.OK, testGvcfLocation) {
      override def addGvcf(
        input: TransferGvcfV1Key,
        transferGvcfV1Metadata: TransferGvcfV1Metadata
      )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
        Future.successful(
          HttpResponse(status = StatusCodes.InternalServerError)
        )
      }
    }
    new MockClioWebClientCantAdd
  }
}
