package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.MemorySearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{ClioDocument, ElasticsearchIndex}
import org.broadinstitute.clio.server.service.MockIndexService
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.UpsertId

abstract class IndexWebServiceSpec[
  TI <: TransferIndex,
  D <: ClioDocument: ElasticsearchIndex
] extends BaseWebserviceSpec {

  val memorySearchDAO = new MemorySearchDAO()

  val webServiceName: String
  val mockService: MockIndexService[TI, D]
  val webService: IndexWebService[TI, D]
  val onPremKey: webService.indexService.transferIndex.KeyType
  val cloudKey: webService.indexService.transferIndex.KeyType

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

  it should "query with a BoGuS project and sample and return nothing" in {
    Post(
      "/query",
      Map("project" -> "testBoGuSproject", "sample_alias" -> "testBoGuSsample")
    ) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "queryall with an empty request" in {
    mockService.queryAllCalls.clear()
    Post("/queryall", Map.empty[String, String]) ~> webService.queryall ~> check {
      status shouldEqual StatusCodes.OK
    }
    mockService.queryAllCalls should have length 1
  }

  it should "queryall without an empty request" in {
    mockService.queryAllCalls.clear()
    Post("/queryall", Map("project" -> "project")) ~> webService.queryall ~> check {
      status shouldEqual StatusCodes.OK
    }
    mockService.queryAllCalls should have length 1
  }

  it should "query with an empty request" in {
    mockService.queryCalls.clear()
    Post("/query", Map.empty[String, String]) ~> webService.query ~> check {
      status shouldEqual StatusCodes.OK
    }
    mockService.queryCalls should have length 1
  }

  it should "query without an empty request" in {
    mockService.queryCalls.clear()
    Post("/query", Map("project" -> "project")) ~> webService.query ~> check {
      status shouldEqual StatusCodes.OK
    }
    mockService.queryCalls should have length 1
  }

  it should "return a JSON schema" in {
    Get("/schema") ~> webService.getSchema ~> check {
      responseAs[Json] should be(webService.indexService.transferIndex.jsonSchema)
    }
  }

  def metadataRouteFromKey(key: webService.indexService.transferIndex.KeyType): String = {
    (Seq("/metadata") ++ key.getUrlSegments).mkString("/")
  }

  def replaceLocationWithBogusInRoute(route: String): String = {
    route
      .replace("OnPrem", "Bogus")
      .replace("GCP", "Bogus")
  }
}
