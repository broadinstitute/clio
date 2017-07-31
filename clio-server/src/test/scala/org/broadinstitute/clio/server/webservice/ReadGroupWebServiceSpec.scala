package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import org.broadinstitute.clio.server.service.SchemaService
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._
import org.scalatest.{FlatSpec, Matchers}

class ReadGroupWebServiceSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest {
  behavior of "ReadGroupWebService"

  it should "postMetadata" in {
    val webService = new MockReadGroupWebService()
    Post("/metadata/v1/barcodeA/2/libraryC", Map("project" -> "testProject")) ~> webService.postMetadata ~> check {
      responseAs[Map[String, String]] should be(empty)
    }
  }

  it should "postMetadataV2 with OnPrem location" in {
    val webService = new MockReadGroupWebService()
    Post(
      "/metadata/v2/barcodeOnPrem/3/libraryOnPrem/OnPrem",
      Map("project" -> "testOnPremLocation")
    ) ~> webService.postMetadata ~> check {
      responseAs[Map[String, String]] should be(empty)
    }
  }

  it should "postMetadataV2 with GCP location" in {
    val webService = new MockReadGroupWebService()
    Post(
      "/metadata/v2/barcodeGCP/4/libraryGCP/GCP",
      Map("project" -> "testGCPlocation")
    ) ~> webService.postMetadata ~> check {
      responseAs[Map[String, String]] should be(empty)
    }
  }

  it should "reject postMetadataV2 with BoGuS location" in {
    val webService = new MockReadGroupWebService()
    Post(
      "/metadata/v2/barcodeBoGuS/5/libraryBoGuS/BoGuS",
      Map("project" -> "testBoGuSlocation")
    ) ~> Route.seal(webService.postMetadata) ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "query with an empty request" in {
    val webService = new MockReadGroupWebService()
    Post("/query/v1", Map.empty[String, String]) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "queryV2 with an empty request" in {
    val webService = new MockReadGroupWebService()
    Post("/query/v2", Map.empty[String, String]) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "query without an empty request" in {
    val webService = new MockReadGroupWebService()
    Post("/query/v1", Map("project" -> "testProject")) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "queryV2 without an empty request" in {
    val webService = new MockReadGroupWebService()
    Post("/query/v2", Map("project" -> "testProject")) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "return a JSON schema" in {
    val webService = new MockReadGroupWebService()
    Get("/schema/v1") ~> webService.getSchema ~> check {
      responseAs[Json] should be(SchemaService.readGroupSchemaJson)
    }
  }

  it should "return a V2 JSON schema" in {
    val webService = new MockReadGroupWebService()
    Get("/schema/v2") ~> webService.getSchema ~> check {
      responseAs[Json] should be(SchemaService.readGroupSchemaJsonV2)
    }
  }
}
