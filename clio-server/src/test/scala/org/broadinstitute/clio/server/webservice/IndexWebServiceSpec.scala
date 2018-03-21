package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{MalformedRequestContentRejection, Route}
import org.broadinstitute.clio.server.dataaccess.MemorySearchDAO
import org.broadinstitute.clio.server.service.MockIndexService
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.model.UpsertId

abstract class IndexWebServiceSpec[
  CI <: ClioIndex
] extends BaseWebserviceSpec {

  val memorySearchDAO = new MemorySearchDAO()

  def webServiceName: String
  def mockService: MockIndexService[CI]
  val webService: IndexWebService[CI]
  def onPremKey: webService.indexService.clioIndex.KeyType
  def cloudKey: webService.indexService.clioIndex.KeyType
  def badMetadataMap: Map[String, String]
  def badQueryInputMap: Map[String, String]

  behavior of webServiceName

  it should "postMetadata with OnPrem location" in {
    mockService.upsertCalls.clear()
    Post(metadataRouteFromKey(onPremKey), Map("notes" -> "some note")) ~> webService.postMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
    mockService.upsertCalls should have length 1
  }

  it should "postMetadata with GCP location" in {
    mockService.upsertCalls.clear()
    Post(metadataRouteFromKey(cloudKey), Map("notes" -> "some note")) ~> webService.postMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
    mockService.upsertCalls should have length 1
  }

  it should "reject postMetadata with BoGuS location" in {
    mockService.upsertCalls.clear()
    val bogusRoute = replaceLocationWithBogusInRoute(
      metadataRouteFromKey(onPremKey)
    )
    Post(bogusRoute, Map("project" -> "testBoGuSlocation")) ~> Route
      .seal(webService.postMetadata) ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    mockService.upsertCalls should have length 0
  }

  it should "reject postMetadata with incorrect data types in body" in {
    mockService.upsertCalls.clear()
    Post(metadataRouteFromKey(onPremKey), badMetadataMap) ~> webService.postMetadata ~> check {
      rejection should matchPattern {
        case MalformedRequestContentRejection(_, _) =>
      }
    }
    mockService.upsertCalls should have length 0
  }

  it should "successfully queryall with an empty request" in {
    mockService.queryAllCalls.clear()
    Post("/queryall", Map.empty[String, String]) ~> webService.queryall ~> check {
      status shouldEqual StatusCodes.OK
    }
    mockService.queryAllCalls should have length 1
  }

  it should "successfully queryall without an empty request" in {
    mockService.queryAllCalls.clear()
    Post("/queryall", Map("project" -> "project")) ~> webService.queryall ~> check {
      status shouldEqual StatusCodes.OK
    }
    mockService.queryAllCalls should have length 1
  }

  it should "reject queryall with incorrect datatypes in body" in {
    mockService.queryAllCalls.clear()
    Post("/queryall", badQueryInputMap) ~> webService.queryall ~> check {
      rejection should matchPattern {
        case MalformedRequestContentRejection(_, _) =>
      }
    }
    mockService.queryAllCalls should have length 0
  }

  it should "successfully query with an empty request" in {
    mockService.queryCalls.clear()
    Post("/query", Map.empty[String, String]) ~> webService.query ~> check {
      status shouldEqual StatusCodes.OK
    }
    mockService.queryCalls should have length 1
  }

  it should "successfully query without an empty request" in {
    mockService.queryCalls.clear()
    Post("/query", Map("project" -> "project")) ~> webService.query ~> check {
      status shouldEqual StatusCodes.OK
    }
    mockService.queryCalls should have length 1
  }

  it should "reject query with incorrect datatypes in body" in {
    mockService.queryCalls.clear()
    Post("/query", badQueryInputMap) ~> webService.query ~> check {
      rejection should matchPattern {
        case MalformedRequestContentRejection(_, _) =>
      }
    }
    mockService.queryCalls should have length 0
  }

  def metadataRouteFromKey(key: webService.indexService.clioIndex.KeyType): String = {
    (Seq("/metadata") ++ key.getUrlSegments).mkString("/")
  }

  def replaceLocationWithBogusInRoute(route: String): String = {
    route
      .replace("OnPrem", "Bogus")
      .replace("GCP", "Bogus")
  }
}
