package org.broadinstitute.clio.client.webclient

import java.net.URI

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.Json
import org.broadinstitute.clio.client.util.{IoUtil, TestData}
import org.broadinstitute.clio.status.model.{StatusInfo, VersionInfo}
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
      TestData.testRequestTimeout,
      TestData.testMaxRetries,
      TestData.fakeTokenGenerator
    )
    with TestData
    with ModelAutoDerivation {

  val health = StatusInfo.Running
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

  override def getSchema(clioIndex: ClioIndex): Future[Json] = {
    if (status.isSuccess()) {
      Future.successful(clioIndex.jsonSchema)
    } else {
      Future.failed(new RuntimeException("Failed to get schema"))
    }
  }

  override def upsert[CI <: ClioIndex](clioIndex: CI)(
    key: clioIndex.KeyType,
    metadata: clioIndex.MetadataType
  ): Future[UpsertId] = {
    if (status.isSuccess()) {
      Future.successful(UpsertId.nextId())
    } else {
      Future.failed(new RuntimeException("Failed to upsert"))
    }
  }

  override def query[CI <: ClioIndex](clioIndex: CI)(
    input: clioIndex.QueryInputType,
    includeDeleted: Boolean
  ): Future[Json] = {
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
      override def upsert[CI <: ClioIndex](clioIndex: CI)(
        key: clioIndex.KeyType,
        metadata: clioIndex.MetadataType
      ): Future[UpsertId] = {
        Future.failed(new RuntimeException("Failed to upsert"))
      }
    }
  }
}
