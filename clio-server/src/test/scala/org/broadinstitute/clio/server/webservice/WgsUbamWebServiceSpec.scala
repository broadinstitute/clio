package org.broadinstitute.clio.server.webservice

import java.net.URI

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import io.circe.Json
import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.MemorySearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentWgsUbam
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import org.broadinstitute.clio.server.service.WgsUbamService
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.TransferUbamV1QueryInput

class WgsUbamWebServiceSpec extends BaseWebserviceSpec {
  behavior of "WgsUbamWebService"

  it should "postMetadata with OnPrem location" in {
    val webService = new MockWgsUbamWebService()
    Post(
      "/metadata/OnPrem/barcodeOnPrem/3/libraryOnPrem",
      Map("project" -> "testOnPremLocation")
    ) ~> webService.postMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
  }

  it should "postMetadata with GCP location" in {
    val webService = new MockWgsUbamWebService()
    Post(
      "/metadata/GCP/barcodeGCP/4/libraryGCP",
      Map("project" -> "testGCPlocation")
    ) ~> webService.postMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
  }

  it should "query with a project and sample and return multiple records" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockWgsUbamWebService(app)
    Post(
      "/metadata/GCP/barcodeGCP/5/libraryGCP",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/GCP/barcodeGCP/6/libraryGCP",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/GCP/barcodeGCP/7/libraryGCP",
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
          TransferUbamV1QueryInput(
            project = Some("testProject1"),
            documentStatus = Some(DocumentStatus.Normal)
          )
        ).map(WgsUbamService.v1QueryConverter.buildQuery)
      )
    }

    Post(
      "/query",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.query ~> check {
      memorySearchDAO.queryCalls should have length 2
      val secondQuery =
        memorySearchDAO.queryCalls(1).asInstanceOf[BoolQueryDefinition]
      secondQuery.must should have length 3
    }
  }

  it should "upsert a record, delete it and then fail to find it with query, but find it with queryall" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockWgsUbamWebService(app)
    Post(
      "/metadata/GCP/FC123/1/lib1",
      Map(
        "project" -> "G123",
        "sample_alias" -> "sample1",
        "ubam_path" -> "gs://path/ubam.bam"
      )
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 1
      val firstUpdate = memorySearchDAO.updateCalls.headOption
        .map(_._1)
        .getOrElse {
          // Doing this .headOption.getOrElse dance because Codacy
          // scolds us for using .head
          fail("Impossible because of the above check")
        }
        .asInstanceOf[DocumentWgsUbam]

      firstUpdate.upsertId should be(responseAs[UpsertId])
      firstUpdate.project should be(Some("G123"))
      firstUpdate.sampleAlias should be(Some("sample1"))
      firstUpdate.ubamPath should be(Some(URI.create("gs://path/ubam.bam")))
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("flowcell_barcode" -> "FC123")) ~> webService.query ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferUbamV1QueryInput(
            flowcellBarcode = Some("FC123"),
            documentStatus = Some(DocumentStatus.Normal)
          )
        ).map(WgsUbamService.v1QueryConverter.buildQuery)
      )
    }

    Post(
      "/metadata/GCP/FC123/1/lib1",
      Map(
        "project" -> "G123",
        "sample_alias" -> "sample1",
        "document_status" -> "Deleted",
        "ubam_path" -> ""
      )
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 2
      val secondUpdate = memorySearchDAO.updateCalls
        .map(_._1)
        .apply(1)
        .asInstanceOf[DocumentWgsUbam]
      secondUpdate.project should be(Some("G123"))
      secondUpdate.sampleAlias should be(Some("sample1"))
      secondUpdate.documentStatus should be(Some(DocumentStatus.Deleted))
      secondUpdate.ubamPath should be(Some(URI.create("")))
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/queryall", Map("flowcell_barcode" -> "FC123")) ~> webService.queryall ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          // From /query call earlier in the test.
          TransferUbamV1QueryInput(
            flowcellBarcode = Some("FC123"),
            documentStatus = Some(DocumentStatus.Normal)
          ),
          // No documentStatus restriction from /queryall
          TransferUbamV1QueryInput(flowcellBarcode = Some("FC123"))
        ).map(WgsUbamService.v1QueryConverter.buildQuery)
      )
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
      "/metadata/BoGuS/barcodeBoGuS/5/libraryBoGuS/",
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
      responseAs[Json] should be(WgsUbamIndex.jsonSchema)
    }
  }
}
