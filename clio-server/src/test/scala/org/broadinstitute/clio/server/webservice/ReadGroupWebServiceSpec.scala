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

  it should "postMetadata with OnPrem location" in {
    val webService = new MockReadGroupWebService()
    Post(
      "/metadata/v1/barcodeOnPrem/3/libraryOnPrem/OnPrem",
      Map("project" -> "testOnPremLocation")
    ) ~> webService.postMetadata ~> check {
      responseAs[Map[String, String]] should be(empty)
    }
  }

  it should "postMetadata with GCP location" in {
    val webService = new MockReadGroupWebService()
    Post(
      "/metadata/v1/barcodeGCP/4/libraryGCP/GCP",
      Map("project" -> "testGCPlocation")
    ) ~> webService.postMetadata ~> check {
      responseAs[Map[String, String]] should be(empty)
    }
  }

  it should "query with a project and sample and return multiple records" in {
    val webService = new MockReadGroupWebService()
    Post(
      "/metadata/v1/barcodeGCP/5/LibraryGCP/GCP",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/v1/barcodeGCP/6/LibraryGCP/GCP",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/v1/barcodeGCP/7/LibraryGCP/GCP",
      Map("project" -> "testProject1", "sample_alias" -> "sample2")
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }

    Post(
      "/query/v1",
      Map("project" -> "testProject1")
    ) ~> webService.query ~> check {
      print(response.toString())
      responseAs[Seq[String]] should not be empty
    }

    Post(
      "/query/v1",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.query ~> check {
      responseAs[Seq[String]] should not be empty
    }
  }

  it should "query with a BoGuS project and sample and return nothing" in {
    val webService = new MockReadGroupWebService()
    Post(
      "/query/v1",
      Map("project" -> "testBoGuSproject", "sample_alias" -> "testBoGuSsample")
    ) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "reject postMetadata with BoGuS location" in {
    val webService = new MockReadGroupWebService()
    Post(
      "/metadata/v1/barcodeBoGuS/5/libraryBoGuS/BoGuS",
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

  it should "query without an empty request" in {
    val webService = new MockReadGroupWebService()
    Post("/query/v1", Map("project" -> "testProject")) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "return a JSON schema" in {
    val webService = new MockReadGroupWebService()
    Get("/schema/v1") ~> webService.getSchema ~> check {
      responseAs[Json] should be(SchemaService.readGroupSchemaJson)
    }
  }
}
