package org.broadinstitute.clio.client.webclient

import java.net.URI

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpCredentials
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.broadinstitute.clio.client.util.{IoUtil, TestData}
import org.broadinstitute.clio.status.model.{
  ServerStatusInfo,
  StatusInfo,
  SystemStatusInfo,
  VersionInfo
}
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.Future

class MockClioWebClient(status: StatusCode, metadataLocationOption: Option[URI])(
  implicit system: ActorSystem
) extends ClioWebClient(
      "localhost",
      TestData.testServerPort,
      false,
      TestData.testMaxQueued,
      TestData.testMaxConcurrent,
      TestData.testRequestTimeout
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

  override def getClioServerHealth: Future[Json] = {
    if (status.isSuccess()) {
      Future.successful(health.asJson)
    } else {
      Future.failed(new RuntimeException("Failed to get server health"))
    }
  }

  override def getClioServerVersion: Future[Json] = {
    if (status.isSuccess()) {
      Future.successful(version.asJson)
    } else {
      Future.failed(new RuntimeException("Failed to get server version"))
    }
  }

  override def getSchema(
    transferIndex: TransferIndex
  )(implicit credentials: HttpCredentials): Future[Json] = {
    if (status.isSuccess()) {
      Future.successful(transferIndex.jsonSchema)
    } else {
      Future.failed(new RuntimeException("Failed to get schema"))
    }
  }

  override def upsert[T](transferIndex: TransferIndex,
                         key: TransferKey,
                         metadata: T)(implicit credentials: HttpCredentials,
                                      encoder: Encoder[T]): Future[UpsertId] = {
    if (status.isSuccess()) {
      Future.successful(UpsertId.nextId())
    } else {
      Future.failed(new RuntimeException("Failed to upsert"))
    }
  }

  override def query[T](
    transferIndex: TransferIndex,
    input: T,
    includeDeleted: Boolean
  )(implicit credentials: HttpCredentials,
    encoder: Encoder[T]): Future[Json] = {
    if (status.isSuccess()) {
      Future.successful(json.getOrElse(Json.fromValues(Seq.empty)))
    } else {
      Future.failed(new RuntimeException("Failed to query"))
    }
  }
}

object MockClioWebClient {
  def failingToUpsert(implicit system: ActorSystem): MockClioWebClient = {
    new MockClioWebClient(status = StatusCodes.OK, None) {
      override def upsert[T](
        transferIndex: TransferIndex,
        key: TransferKey,
        metadata: T
      )(implicit credentials: HttpCredentials,
        encoder: Encoder[T]): Future[UpsertId] = {
        Future.failed(new RuntimeException("Failed to upsert"))
      }
    }
  }
}
