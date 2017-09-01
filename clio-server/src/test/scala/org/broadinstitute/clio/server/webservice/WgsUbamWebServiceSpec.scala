package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.MemorySearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentWgsUbam
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._
import org.broadinstitute.clio.transfer.model.TransferWgsUbamV1QueryInput
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.model.DocumentStatus

import org.scalatest.{FlatSpec, Matchers}

import java.util.UUID

class WgsUbamWebServiceSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest {
  behavior of "WgsUbamWebService"

  it should "postMetadata with OnPrem location" in {
    val webService = new MockWgsUbamWebService()
    Post(
      "/metadata/barcodeOnPrem/3/libraryOnPrem/OnPrem",
      Map("project" -> "testOnPremLocation")
    ) ~> webService.postMetadata ~> check {
      responseAs[String] should not be (empty)
    }
  }

  it should "postMetadata with GCP location" in {
    val webService = new MockWgsUbamWebService()
    Post(
      "/metadata/barcodeGCP/4/libraryGCP/GCP",
      Map("project" -> "testGCPlocation")
    ) ~> webService.postMetadata ~> check {
      responseAs[String] should not be (empty)
    }
  }

  it should "query with a project and sample and return multiple records" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockWgsUbamWebService(app)
    Post(
      "/metadata/barcodeGCP/5/LibraryGCP/GCP",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/barcodeGCP/6/LibraryGCP/GCP",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/barcodeGCP/7/LibraryGCP/GCP",
      Map("project" -> "testProject1", "sample_alias" -> "sample2")
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("project" -> "testProject1")) ~> webService.query ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferWgsUbamV1QueryInput(
            project = Some("testProject1"),
            documentStatus = Some(DocumentStatus.Normal)
          )
        )
      )
    }

    Post(
      "/query",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.query ~> check {
      memorySearchDAO.queryCalls should have length 2
      val secondQuery =
        memorySearchDAO.queryCalls(1).asInstanceOf[TransferWgsUbamV1QueryInput]
      secondQuery.project should be(Some("testProject1"))
      secondQuery.sampleAlias should be(Some("sample1"))
    }
  }

  it should "upsert a record, delete it and then fail to find it with query, but find it with queryall" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockWgsUbamWebService(app)
    Post(
      "/metadata/FC123/1/lib1/GCP",
      Map(
        "project" -> "G123",
        "sample_alias" -> "sample1",
        "ubam_path" -> "gs://path/ubam.bam"
      )
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 1
      val firstUpdate = memorySearchDAO.updateCalls.headOption
        .map(_._2)
        .getOrElse {
          // Doing this .headOption.getOrElse dance because Codacy
          // scolds us for using .head
          fail("Impossible because of the above check")
        }
        .asInstanceOf[DocumentWgsUbam]

      firstUpdate.clioId should be(responseAs[UUID])
      firstUpdate.project should be(Some("G123"))
      firstUpdate.sampleAlias should be(Some("sample1"))
      firstUpdate.ubamPath should be(Some("gs://path/ubam.bam"))
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("flowcell_barcode" -> "FC123")) ~> webService.query ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferWgsUbamV1QueryInput(
            flowcellBarcode = Some("FC123"),
            documentStatus = Some(DocumentStatus.Normal)
          )
        )
      )
    }

    Post(
      "/metadata/FC123/1/lib1/GCP",
      Map(
        "project" -> "G123",
        "sample_alias" -> "sample1",
        "document_status" -> "Deleted",
        "ubam_path" -> ""
      )
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 2
      val secondUpdate = memorySearchDAO
        .updateCalls(1)
        .asInstanceOf[(_, DocumentWgsUbam, _)]
      secondUpdate._2.project should be(Some("G123"))
      secondUpdate._2.sampleAlias should be(Some("sample1"))
      secondUpdate._2.documentStatus should be(Some(DocumentStatus.Deleted))
      secondUpdate._2.ubamPath should be(Some(""))
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/queryall", Map("flowcell_barcode" -> "FC123")) ~> webService.queryall ~> check {
      memorySearchDAO.queryCalls should have length 2
      val secondQuery = memorySearchDAO
        .queryCalls(1)
        .asInstanceOf[TransferWgsUbamV1QueryInput]
      secondQuery.flowcellBarcode should be(Some("FC123"))
    }

  }

  it should "query with a BoGuS project and sample and return nothing" in {
    val webService = new MockWgsUbamWebService()
    Post(
      "/query",
      Map("project" -> "testBoGuSproject", "sample_alias" -> "testBoGuSsample")
    ) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "reject postMetadata with BoGuS location" in {
    val webService = new MockWgsUbamWebService()
    Post(
      "/metadata/barcodeBoGuS/5/libraryBoGuS/BoGuS",
      Map("project" -> "testBoGuSlocation")
    ) ~> Route.seal(webService.postMetadata) ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "query with an empty request" in {
    val webService = new MockWgsUbamWebService()
    Post("/query", Map.empty[String, String]) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "query without an empty request" in {
    val webService = new MockWgsUbamWebService()
    Post("/query", Map("project" -> "testProject")) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "return a JSON schema" in {
    val webService = new MockWgsUbamWebService()
    Get("/schema") ~> webService.getSchema ~> check {
      responseAs[Json] should be(JsonSchemas.WgsUbam)
    }
  }
}
