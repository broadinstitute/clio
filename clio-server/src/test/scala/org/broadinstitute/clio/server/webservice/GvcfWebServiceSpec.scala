package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.MemorySearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentGvcf
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._
import org.broadinstitute.clio.transfer.model.TransferGvcfV1QueryInput
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}
import org.scalatest.{FlatSpec, Matchers}
import java.util.UUID

class GvcfWebServiceSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest {
  behavior of "GvcfWebService"

  it should "postMetadata with OnPrem location" in {
    val webService = new MockGvcfWebService()
    Post("/metadata/OnPrem/proj0/sample_alias0/1", Map("notes" -> "some note")) ~> webService.gvcfPostMetadata ~> check {
      responseAs[String] should not be (empty)
    }
  }

  it should "postMetadata with GCP location" in {
    val webService = new MockGvcfWebService()
    Post("/metadata/GCP/proj0/sample_alias0/1", Map("notes" -> "some note")) ~> webService.gvcfPostMetadata ~> check {
      responseAs[String] should not be (empty)
    }
  }

  it should "query with a project and sample and return multiple records" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockGvcfWebService(app)
    Post(
      "/metadata/OnPrem/proj0/sample_alias0/1",
      Map("notes" -> "some note", "gvcf_path" -> "gs://path/gvcf.gvcf")
    ) ~> webService.gvcfPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/OnPrem/proj1/sample_alias0/1",
      Map("notes" -> "some note", "gvcf_path" -> "gs://path/gvcf.gvcf")
    ) ~> webService.gvcfPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/OnPrem/proj2/sample_alias0/1",
      Map("notes" -> "some note", "gvcf_path" -> "gs://path/gvcf.gvcf")
    ) ~> webService.gvcfPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("project" -> "proj0")) ~> webService.gvcfQuery ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferGvcfV1QueryInput(
            project = Some("proj0"),
            documentStatus = Some(DocumentStatus.Normal)
          )
        )
      )
    }

    Post(
      "/query",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.gvcfQuery ~> check {
      memorySearchDAO.queryCalls should have length 2
      val secondQuery =
        memorySearchDAO.queryCalls(1).asInstanceOf[TransferGvcfV1QueryInput]
      secondQuery.project should be(Some("testProject1"))
      secondQuery.sampleAlias should be(Some("sample1"))
    }
  }

  it should "upsert a record, delete it and then fail to find it with query, but find it with queryall" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockGvcfWebService(app)
    Post(
      "/metadata/GCP/proj1/sample_alias0/1",
      Map(
        "gvcf_md5" -> "abcgithashdef",
        "notes" -> "some note",
        "gvcf_path" -> "gs://path/gvcf.gvcf"
      )
    ) ~> webService.gvcfPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 1
      val firstUpdate = memorySearchDAO.updateCalls.headOption
        .map(_._2)
        .getOrElse {
          // Doing this .headOption.getOrElse dance because Codacy
          // scolds us for using .head
          fail("Impossible because of the above check")
        }
        .asInstanceOf[DocumentGvcf]

      firstUpdate.clioId should be(responseAs[UUID])
      firstUpdate.gvcfMd5 should be(Some("abcgithashdef"))
      firstUpdate.notes should be(Some("some note"))
      firstUpdate.gvcfPath should be(Some("gs://path/gvcf.gvcf"))
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("location" -> "GCP")) ~> webService.gvcfQuery ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferGvcfV1QueryInput(
            location = Some(Location.GCP),
            documentStatus = Some(DocumentStatus.Normal)
          )
        )
      )
    }

    Post(
      "/metadata/GCP/proj0/alias/2",
      Map(
        "gvcf_md5" -> "abcgithashdef",
        "notes" -> "some note",
        "document_status" -> "Deleted",
        "gvcf_path" -> ""
      )
    ) ~> webService.gvcfPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 2
      val secondUpdate = memorySearchDAO
        .updateCalls(1)
        .asInstanceOf[(_, DocumentGvcf, _)]
      secondUpdate._2.gvcfMd5 should be(Some("abcgithashdef"))
      secondUpdate._2.notes should be(Some("some note"))
      secondUpdate._2.documentStatus should be(Some(DocumentStatus.Deleted))
      secondUpdate._2.gvcfPath should be(Some(""))
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/queryall", Map("location" -> "GCP")) ~> webService.gvcfQueryall ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          // From /query call earlier in the test.
          TransferGvcfV1QueryInput(
            location = Some(Location.GCP),
            documentStatus = Some(DocumentStatus.Normal)
          ),
          // No documentStatus restriction from /queryall
          TransferGvcfV1QueryInput(location = Some(Location.GCP))
        )
      )
    }
  }

  it should "query with a BoGuS project and sample and return nothing" in {
    val webService = new MockGvcfWebService()
    Post(
      "/query",
      Map("project" -> "testBoGuSproject", "sample_alias" -> "testBoGuSsample")
    ) ~> webService.gvcfQuery ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "reject postMetadata with BoGuS location" in {
    val webService = new MockGvcfWebService()
    Post("/metadata/BoGuS/proj0/alias/2", Map("project" -> "testBoGuSlocation")) ~> Route
      .seal(webService.gvcfPostMetadata) ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "query with an empty request" in {
    val webService = new MockGvcfWebService()
    Post("/query", Map.empty[String, String]) ~> webService.gvcfQuery ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "query without an empty request" in {
    val webService = new MockGvcfWebService()
    Post("/query", Map("project" -> "testProject")) ~> webService.gvcfQuery ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "return a JSON schema" in {
    val webService = new MockGvcfWebService()
    Get("/schema") ~> webService.gvcfGetSchema ~> check {
      responseAs[Json] should be(JsonSchemas.Gvcf)
    }
  }
}