package org.broadinstitute.clio.client.webclient

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpCredentials
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.{Encoder, Json, Printer}
import org.broadinstitute.clio.client.util.{IoUtil, TestData}
import org.broadinstitute.clio.status.model.{
  ServerStatusInfo,
  StatusInfo,
  SystemStatusInfo,
  VersionInfo
}
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.ModelAutoDerivation

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
    with TestData
    with ModelAutoDerivation {

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

  override def getSchema(
    transferIndex: TransferIndex
  )(implicit credentials: HttpCredentials): Future[HttpResponse] = {
    Future.successful(
      HttpResponse(
        status = status,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          transferIndex.jsonSchema.pretty(implicitly[Printer])
        )
      )
    )
  }

  override def upsert[T](
    transferIndex: TransferIndex,
    key: TransferKey,
    metadata: T
  )(implicit credentials: HttpCredentials,
    encoder: Encoder[T]): Future[HttpResponse] = {
    Future.successful(HttpResponse(status = status))
  }

  override def query[T](
    transferIndex: TransferIndex,
    input: T,
    includeDeleted: Boolean
  )(implicit credentials: HttpCredentials,
    encoder: Encoder[T]): Future[HttpResponse] = {
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
    new MockClioWebClient(status = StatusCodes.InternalServerError, None)

  def returningWgsUbam(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, testWgsUbamLocation)
  }

  def returningTwoWgsUbams(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, testTwoWgsUbamsLocation)
  }

  def failingToUpsert(implicit system: ActorSystem): MockClioWebClient = {
    class MockClioWebClientCantUpsert
        extends MockClioWebClient(status = StatusCodes.OK, None) {
      override def upsert[T](
        transferIndex: TransferIndex,
        key: TransferKey,
        metadata: T
      )(implicit credentials: HttpCredentials,
        encoder: Encoder[T]): Future[HttpResponse] = {
        Future.successful(
          HttpResponse(status = StatusCodes.InternalServerError)
        )
      }
    }
    new MockClioWebClientCantUpsert
  }

  def returningInternalErrorGvcf(implicit system: ActorSystem) =
    new MockClioWebClient(status = StatusCodes.InternalServerError, None)

  def returningGvcf(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, testGvcfLocation)
  }

  def returningGvcfOnlyMetrics(
    implicit system: ActorSystem
  ): MockClioWebClient = {
    new MockClioWebClient(
      status = StatusCodes.OK,
      testGvcfMetadataOnlyMetricsLocation
    )
  }

  def returningTwoGvcfs(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, testTwoGvcfsLocation)
  }
}
