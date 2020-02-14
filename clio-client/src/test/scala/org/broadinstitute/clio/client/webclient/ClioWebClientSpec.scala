package org.broadinstitute.clio.client.webclient

import java.time.OffsetDateTime
import java.util.concurrent.TimeoutException

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import io.circe.{Encoder, Json}
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.status.model.{
  ClioStatus,
  SearchStatus,
  StatusInfo,
  VersionInfo
}
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}
import org.scalamock.scalatest.AsyncMockFactory

import scala.concurrent.duration._

class ClioWebClientSpec extends BaseClientSpec with AsyncMockFactory {

  behavior of "ClioWebClient"

  private val timeout = 1.second

  it should "dispatch requests" in {
    val req = HttpRequest(uri = "my-cool-uri")
    val response = "Hello World!"

    val flow = Flow[HttpRequest].map { r =>
      r should be(req)

      val entity = HttpEntity(response)
      HttpResponse(entity = entity)
    }

    val client =
      new ClioWebClient(flow, timeout, 0, stub[CredentialsGenerator])

    client.dispatchRequest(req, false).runFold(ByteString.empty)(_ ++ _).map {
      _.decodeString("UTF-8") should be(response)
    }
  }

  it should "add an OAuth header to dispatched requests" in {
    val req = HttpRequest(uri = "my-cool-uri")
    val response = "Hello World!"

    val token = OAuth2BearerToken("fake")
    val generator = mock[CredentialsGenerator]
    (generator.generateCredentials _).expects().returning(token)

    val flow = Flow[HttpRequest].map { r =>
      val authHeader = r.headers.collectFirst { case a: Authorization => a }
      authHeader should be(Some(Authorization(token)))

      val entity = HttpEntity(response)
      HttpResponse(entity = entity)
    }

    val client = new ClioWebClient(flow, timeout, 0, generator)

    client.dispatchRequest(req, true).runFold(ByteString.empty)(_ ++ _).map {
      _.decodeString("UTF-8") should be(response)
    }
  }

  it should "raise an error when a response has an error code" in {
    val code = StatusCodes.BadRequest
    val err = "Nope"

    val flow = Flow[HttpRequest].map { _ =>
      HttpResponse(status = code, entity = HttpEntity(err))
    }

    val client =
      new ClioWebClient(flow, timeout, 0, stub[CredentialsGenerator])

    recoverToExceptionIf[ClioWebClient.FailedResponse] {
      client.dispatchRequest(HttpRequest(), false).runWith(Sink.ignore)
    }.map { ex =>
      ex.statusCode should be(code)
      ex.entityBody should be(err)
    }
  }

  it should "time out requests that take too long" in {

    val flow = Flow[HttpRequest].delay(timeout * 2).map(_ => HttpResponse())
    val client =
      new ClioWebClient(flow, timeout, 0, stub[CredentialsGenerator])

    recoverToSucceededIf[TimeoutException] {
      client.dispatchRequest(HttpRequest(), false).runWith(Sink.ignore)
    }
  }

  it should "retry requests that fail to complete" in {

    val retries = 2
    var retriesSoFar = 0

    val response = "Hello World!"

    val flow = Flow[HttpRequest].flatMapConcat { _ =>
      val src = Source.single(HttpResponse(entity = HttpEntity(response)))

      if (retriesSoFar < retries) {
        retriesSoFar += 1
        src.delay(timeout * 2)
      } else {
        src
      }
    }

    val client =
      new ClioWebClient(flow, timeout, retries, stub[CredentialsGenerator])

    client.dispatchRequest(HttpRequest(), false).runFold(ByteString.empty)(_ ++ _).map {
      _.decodeString("UTF-8") should be(response)
    }
  }

  it should "raise an error if a response entity's stream goes idle for too long" in {
    val data = Source
      .single(ByteString("Hello"))
      .concat(Source.single(ByteString("World!")).delay(timeout * 2))
    val entity = HttpEntity(contentType = ContentTypes.`text/plain(UTF-8)`, data)
    val flow = Flow[HttpRequest].map(_ => HttpResponse(entity = entity))

    val client =
      new ClioWebClient(flow, timeout, 0, stub[CredentialsGenerator])
    recoverToSucceededIf[TimeoutException] {
      client.dispatchRequest(HttpRequest(), false).runWith(Sink.ignore)
    }
  }

  def jsonResponse[A: Encoder](body: A): HttpResponse = {
    val entity = HttpEntity(
      ContentTypes.`application/json`,
      body.asJson.printWith(defaultPrinter)
    )
    HttpResponse(entity = entity)
  }

  it should "get server health as JSON" in {

    val response = StatusInfo(ClioStatus.Recovering, SearchStatus.OK)

    val flow = Flow[HttpRequest].map { req =>
      req.uri.path.toString should be(s"/${ApiConstants.healthString}")
      jsonResponse(response)
    }

    val client =
      new ClioWebClient(flow, timeout, 1, stub[CredentialsGenerator])

    client.getClioServerHealth.runWith(Sink.head).map {
      _.as[StatusInfo] should be(Right(response))
    }
  }

  it should "get server version as JSON" in {

    val response = VersionInfo("the-version")

    val flow = Flow[HttpRequest].map { req =>
      req.uri.path.toString should be(s"/${ApiConstants.versionString}")
      jsonResponse(response)
    }

    val client =
      new ClioWebClient(flow, timeout, 0, stub[CredentialsGenerator])

    client.getClioServerVersion.runWith(Sink.head).map {
      _.as[VersionInfo] should be(Right(response))
    }
  }

  Seq(true, false).foreach { b =>
    it should behave like upsertTest(b)
    it should behave like queryTest(b)
    it should behave like getMetadataTest(b)
  }

  def upsertTest(force: Boolean): Unit = {
    it should s"upsert metadata with force=$force" in {

      val now = OffsetDateTime.now()
      val index = ModelMockIndex()
      val key = ModelMockKey(10L, "abcdefg")
      val metadata = ModelMockMetadata(
        mockFieldDate = Some(now),
        mockDocumentStatus = Some(DocumentStatus.Normal)
      )
      val response = UpsertId.nextId()

      val expectedSegments = Seq.concat(
        Seq(ApiConstants.apiString, "v1", index.urlSegment, ApiConstants.metadataString),
        key.getUrlSegments
      )

      val flow = Flow[HttpRequest].map { req =>
        req.uri.path.toString should be(expectedSegments.mkString("/", "/", ""))
        req.entity should be(
          HttpEntity(
            ContentTypes.`application/json`,
            metadata.asJson.printWith(defaultPrinter)
          )
        )
        req.uri.query().get(ApiConstants.forceString) should be(Some(force.toString))
        jsonResponse(response)
      }

      val generator = mock[CredentialsGenerator]
      (generator.generateCredentials _).expects().returning(OAuth2BearerToken("fake"))

      val client = new ClioWebClient(flow, timeout, 0, generator)

      client.upsert(index)(key, metadata, force).runWith(Sink.head).map {
        _.as[UpsertId] should be(Right(response))
      }
    }
  }

  def queryTest(includeAllStatuses: Boolean): Unit = {
    it should s"query metadata with includeAllStatuses=$includeAllStatuses" in {

      val index = ModelMockIndex()

      val query = ModelMockQueryInput(mockFieldInt = Some(1))
      val expectedRequestBody = (
        if (includeAllStatuses) {
          query
        } else {
          query.withDocumentStatus(Some(DocumentStatus.Normal))
        }
      ).asJson.printWith(defaultPrinter)

      val keys = Seq.tabulate(3)(i => ModelMockKey(i.toLong, "abcdefg"))
      val expectedOut =
        keys.map { k =>
          k.asJson.deepMerge(query.asJson.mapObject(_.filter(_._2 != Json.Null)))
        }

      val expectedSegments = Seq(
        ApiConstants.apiString,
        "v1",
        index.urlSegment,
        ApiConstants.queryString
      )

      val flow = Flow[HttpRequest].map { req =>
        req.uri.path.toString should be(expectedSegments.mkString("/", "/", ""))
        req.entity should be(
          HttpEntity(
            ContentTypes.`application/json`,
            expectedRequestBody
          )
        )
        jsonResponse(expectedOut)
      }

      val generator = mock[CredentialsGenerator]
      (generator.generateCredentials _).expects().returning(OAuth2BearerToken("fake"))

      val client = new ClioWebClient(flow, timeout, 0, generator)

      client
        .simpleQuery(index)(query, includeAllStatuses)
        .runWith(Sink.seq)
        .map {
          _ should contain theSameElementsAs expectedOut
        }
    }
  }

  def getMetadataTest(includeDeleted: Boolean): Unit = {
    it should s"get the metadata for a key with includeDeleted=$includeDeleted" in {
      val index = ModelMockIndex()

      val key = ModelMockKey(1L, "abcdefg")
      val metadata = ModelMockMetadata(mockFieldDouble = Some(1.23))
      val expectedOut =
        key.asJson.deepMerge(metadata.asJson.mapObject(_.filter(_._2 != Json.Null)))

      val expectedSegments = Seq(
        ApiConstants.apiString,
        "v1",
        index.urlSegment,
        ApiConstants.queryString
      )

      val expectedRequestBody: String = (
        if (includeDeleted) {
          key.asJson
        } else {
          Metadata.jsonWithDocumentStatus(key.asJson, DocumentStatus.Normal)
        }
      ).printWith(defaultPrinter)

      val flow = Flow[HttpRequest].map { req =>
        req.uri.path.toString should be(expectedSegments.mkString("/", "/", ""))
        req.entity should be(
          HttpEntity(
            ContentTypes.`application/json`,
            expectedRequestBody
          )
        )
        jsonResponse(Seq(expectedOut))
      }

      val generator = mock[CredentialsGenerator]
      (generator.generateCredentials _).expects().returning(OAuth2BearerToken("fake"))

      val client = new ClioWebClient(flow, timeout, 0, generator)

      client.getMetadataForKey(index)(key, includeDeleted).runWith(Sink.head).map {
        _ should be(metadata)
      }
    }
  }

  it should "raise an error if > 1 document is returned for a key" in {
    val index = ModelMockIndex()

    val key = ModelMockKey(1L, "abcdefg")
    val metadata = Seq.tabulate(3) { i =>
      ModelMockMetadata(mockFieldDouble = Some(1.23), mockFieldInt = Some(i))
    }
    val out = metadata.map { m =>
      key.asJson.deepMerge(m.asJson.mapObject(_.filter(_._2 != Json.Null)))
    }

    val flow = Flow[HttpRequest].map(_ => jsonResponse(out))

    val generator = mock[CredentialsGenerator]
    (generator.generateCredentials _).expects().returning(OAuth2BearerToken("fake"))

    val client = new ClioWebClient(flow, timeout, 0, generator)

    recoverToSucceededIf[IllegalStateException] {
      client.getMetadataForKey(index)(key, false).runWith(Sink.head)
    }
  }
}
