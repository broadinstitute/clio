package org.broadinstitute.client.webclient

import akka.http.scaladsl.model.{
  HttpEntity,
  HttpResponse,
  StatusCode,
  StatusCodes
}
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

import scala.concurrent.Future

class MockClioWebClient(status: StatusCode)(implicit system: ActorSystem)
    extends ClioWebClient("localhost", 8080, false)
    with TestData {

  val version: String = """|{
                           |  "version" : "0.0.1"
                           |}""".stripMargin

  val json: Json = parse(IoUtil.readMetadata(metadataFileLocation)) match {
    case Right(value) => value
    case Left(parsingFailure) =>
      throw parsingFailure
  }

  override def getClioServerVersion: Future[HttpResponse] = {
    Future.successful(
      HttpResponse(status = status, entity = HttpEntity(version))
    )
  }

  override def addWgsUbam(
    bearerToken: String,
    input: TransferWgsUbamV1Key,
    transferWgsUbamV1Metadata: TransferWgsUbamV1Metadata
  ): Future[HttpResponse] = {
    Future.successful(HttpResponse(status = status))
  }

  override def queryWgsUbam(
    bearerToken: String,
    input: TransferWgsUbamV1QueryInput
  ): Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(json.pretty(implicitly))
      )
    )
  }
}

object MockClioWebClient {
  def returningOk(implicit system: ActorSystem) =
    new MockClioWebClient(status = StatusCodes.OK)

  def returningInternalError(implicit system: ActorSystem) =
    new MockClioWebClient(status = StatusCodes.InternalServerError)
}