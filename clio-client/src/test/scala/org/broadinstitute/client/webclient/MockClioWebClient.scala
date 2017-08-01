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
  TransferReadGroupV1Key,
  TransferReadGroupV1Metadata,
  TransferReadGroupV1QueryInput
}

import scala.concurrent.Future

class MockClioWebClient(status: StatusCode)
    extends ClioWebClient
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

  override def addReadGroupBam(
    bearerToken: String,
    input: TransferReadGroupV1Key,
    transferReadGroupV1Metadata: TransferReadGroupV1Metadata
  ): Future[HttpResponse] = {
    Future.successful(HttpResponse(status = status))
  }

  override def queryReadGroupBam(
    bearerToken: String,
    input: TransferReadGroupV1QueryInput
  ): Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(json.pretty(implicitly))
      )
    )
  }
}

object OkReturningMockClioWebClient
    extends MockClioWebClient(status = StatusCodes.OK)
    with TestData

object InternalErrorReturningMockClioWebClient
    extends MockClioWebClient(status = StatusCodes.InternalServerError)
    with TestData
