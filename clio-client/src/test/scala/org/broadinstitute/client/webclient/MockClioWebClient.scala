package org.broadinstitute.client.webclient

import akka.http.scaladsl.model._
import io.circe.Json
import io.circe.parser.parse
import org.broadinstitute.client.util.TestData
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput
}
import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken

import scala.concurrent.Future

class MockClioWebClient(
  status: StatusCode,
  metadataLocationOption: Option[String]
)(implicit system: ActorSystem)
    extends ClioWebClient("localhost", 8080, false)
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

  override def addWgsUbam(
    bearerToken: String,
    input: TransferWgsUbamV1Key,
    transferWgsUbamV1Metadata: TransferWgsUbamV1Metadata
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    Future.successful(HttpResponse(status = status))
  }

  override def queryWgsUbam(
    bearerToken: String,
    input: TransferWgsUbamV1QueryInput
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          json.map(_.pretty(implicitly)).getOrElse("")
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
  def returningNoWgsUbam(implicit system: ActorSystem): MockClioWebClient = {
    class MockClioWebClientNoReturn
        extends MockClioWebClient(status = StatusCodes.OK, None) {
      override def queryWgsUbam(
        bearerToken: String,
        input: TransferWgsUbamV1QueryInput
      ): Future[HttpResponse] = {
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
        extends MockClioWebClient(
          status = StatusCodes.InternalServerError,
          None
        ) {
      override def addWgsUbam(
        bearerToken: String,
        input: TransferWgsUbamV1Key,
        transferWgsUbamV1Metadata: TransferWgsUbamV1Metadata
      ): Future[HttpResponse] = {
        Future.successful(
          HttpResponse(status = StatusCodes.InternalServerError)
        )
      }
    }
    new MockClioWebClientCantAdd
  }
}
