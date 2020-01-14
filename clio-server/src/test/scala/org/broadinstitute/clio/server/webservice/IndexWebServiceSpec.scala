package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{MalformedRequestContentRejection, Route}
import akka.stream.scaladsl.Source
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.MemorySearchDAO
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.model.UpsertId
import org.broadinstitute.clio.transfer.model.ApiConstants._
import org.scalamock.scalatest.MockFactory
abstract class IndexWebServiceSpec[
  CI <: ClioIndex
] extends BaseWebserviceSpec
    with MockFactory {

  val memorySearchDAO = new MemorySearchDAO()
  def webServiceName: String
  val mockService: IndexService[CI]
  val webService: IndexWebService[CI]
  def onPremKey: webService.indexService.clioIndex.KeyType
  def cloudKey: webService.indexService.clioIndex.KeyType
  def badMetadataMap: Map[String, String]
  def badQueryInputMap: Map[String, String]
  def emptyOutput: Json

  behavior of webServiceName

  it should "postMetadata with OnPrem location" in {
    expectUpsert()
    Post(metadataRouteFromKey(onPremKey), Map("notes" -> "some note")) ~> webService.postMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
  }

  it should "postMetadata with GCP location" in {
    expectUpsert()
    Post(metadataRouteFromKey(cloudKey), Map("notes" -> "some note")) ~> webService.postMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
  }

  it should "reject postMetadata with BoGuS location" in {
    expectNoUpsert()
    val bogusRoute = replaceLocationWithBogusInRoute(
      metadataRouteFromKey(onPremKey)
    )
    Post(bogusRoute, Map("project" -> "testBoGuSlocation")) ~> Route
      .seal(webService.postMetadata) ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "reject postMetadata with incorrect data types in body" in {
    expectNoUpsert()
    Post(metadataRouteFromKey(onPremKey), badMetadataMap) ~> webService.postMetadata ~> check {
      rejection should matchPattern {
        case MalformedRequestContentRejection(_, _) =>
      }
    }
  }

  it should "successfully query with an empty request" in {
    expectQueryMetadata()
    Post(s"/$queryString", Map.empty[String, String]) ~> webService.query ~> check {
      val response = responseAs[String]
      info(s"responseAs[String] == '${response}'")
      response shouldEqual "[]"
      status shouldEqual StatusCodes.OK
    }
  }

  it should "successfully query without an empty request" in {
    expectQueryMetadata()
    Post(s"/$queryString", Map("project" -> "project")) ~> webService.query ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "reject query with incorrect datatypes in body" in {
    expectNoQueryMetadata()
    Post(s"/$queryString", badQueryInputMap) ~> webService.query ~> check {
      rejection should matchPattern {
        case MalformedRequestContentRejection(_, _) =>
      }
    }
  }

  it should "successfully submit an arbitrary json object as a raw query" in {
    (mockService
      .rawQuery(_: JsonObject))
      .expects(*)
      .returns(Source.single(emptyOutput))
    Post("/rawquery", JsonObject(("key", "this is Json".asJson))) ~> webService.rawquery ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "reject raw query with invalid json in body" in {
    (mockService
      .rawQuery(_: JsonObject))
      .expects(*)
      .never
    Post("/rawquery", "{{))(\"invalid json") ~> webService.rawquery ~> check {
      rejection should matchPattern {
        case MalformedRequestContentRejection(_, _) =>
      }
    }
  }

  def metadataRouteFromKey(key: webService.indexService.clioIndex.KeyType): String = {
    (Seq(s"/$metadataString") ++ key.getUrlSegments).mkString("/")
  }

  def replaceLocationWithBogusInRoute(route: String): String = {
    route
      .replace("OnPrem", "Bogus")
      .replace("GCP", "Bogus")
  }

  private def expectUpsert() = {
    (
      mockService
        .upsertMetadata(
          _: mockService.clioIndex.KeyType,
          _: mockService.clioIndex.MetadataType,
          _: Boolean
        )
      )
      .expects(*, *, *)
      .returns(Source.single(UpsertId.nextId()))
  }

  private def expectNoUpsert() = {
    (
      mockService
        .upsertMetadata(
          _: mockService.clioIndex.KeyType,
          _: mockService.clioIndex.MetadataType,
          _: Boolean
        )
      )
      .expects(*, *, *)
      .never
  }

  private def expectQueryMetadata() = {
    (mockService
      .queryMetadata(
        _: mockService.clioIndex.QueryInputType
      ))
      .expects(*)
      .returns(Source.single(emptyOutput))
  }

  private def expectNoQueryMetadata() = {
    (mockService
      .queryMetadata(
        _: mockService.clioIndex.QueryInputType
      ))
      .expects(*)
      .never
  }
}
