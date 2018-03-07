package org.broadinstitute.clio.server.webservice

import java.net.URI

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import io.circe.Json
import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.MemorySearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.service.WgsCramService
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1QueryInput,
  WgsCramExtensions
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}

class WgsCramWebServiceSpec extends BaseWebserviceSpec with ModelAutoDerivation {
  behavior of "WgsCramWebService"

  it should "postMetadata with OnPrem location" in {
    val webService = new MockWgsCramWebService()
    Post("/metadata/OnPrem/proj0/sample_alias0/1", Map("notes" -> "some note")) ~> webService.cramPostMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
  }

  it should "postMetadata with GCP location" in {
    val webService = new MockWgsCramWebService()
    Post("/metadata/GCP/proj0/sample_alias0/1", Map("notes" -> "some note")) ~> webService.cramPostMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
  }

  it should "query with a project and sample and return multiple records" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockWgsCramWebService(app)
    Post(
      "/metadata/OnPrem/proj0/sample_alias0/1",
      Map(
        "notes" -> "some note",
        "cram_path" -> s"gs://path/cram${WgsCramExtensions.CramExtension}"
      )
    ) ~> webService.cramPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/OnPrem/proj1/sample_alias0/1",
      Map(
        "notes" -> "some note",
        "cram_path" -> s"gs://path/cram${WgsCramExtensions.CramExtension}"
      )
    ) ~> webService.cramPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/OnPrem/proj2/sample_alias0/1",
      Map(
        "notes" -> "some note",
        "cram_path" -> s"gs://path/cram${WgsCramExtensions.CramExtension}"
      )
    ) ~> webService.cramPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("project" -> "proj0")) ~> webService.cramQuery ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferWgsCramV1QueryInput(
            project = Some("proj0"),
            documentStatus = Some(DocumentStatus.Normal)
          )
        ).map(WgsCramService.v1QueryConverter.buildQuery(_)(ElasticsearchIndex.WgsCram))
      )
    }

    Post(
      "/query",
      Map("project" -> "testProject1", "sample_alias" -> "sample1")
    ) ~> webService.cramQuery ~> check {
      memorySearchDAO.queryCalls should have length 2
      val secondQuery =
        memorySearchDAO.queryCalls(1).asInstanceOf[BoolQueryDefinition]
      secondQuery.must should have length 3
    }
  }

  it should "upsert a record, delete it and then fail to find it with query, but find it with queryall" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockWgsCramWebService(app)
    Post(
      "/metadata/GCP/proj1/sample_alias0/1",
      Map(
        "cram_md5" -> "abcgithashdef",
        "notes" -> "some note",
        "cram_path" -> s"gs://path/cram${WgsCramExtensions.CramExtension}"
      )
    ) ~> webService.cramPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 1
      val firstUpdate = memorySearchDAO.updateCalls.headOption
        .flatMap(_._1.headOption)
        .getOrElse {
          // Doing this .headOption.getOrElse dance because Codacy
          // scolds us for using .head
          fail("Impossible because of the above check")
        }

      ElasticsearchIndex.getUpsertId(firstUpdate) should be(responseAs[UpsertId])
      getStringByName(firstUpdate, "cram_md5") should be("abcgithashdef")
      getStringByName(firstUpdate, "notes") should be("some note")
      getUriByName(firstUpdate, "cram_path") should be(
        URI.create(s"gs://path/cram${WgsCramExtensions.CramExtension}")
      )
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("location" -> "GCP")) ~> webService.cramQuery ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferWgsCramV1QueryInput(
            location = Some(Location.GCP),
            documentStatus = Some(DocumentStatus.Normal)
          )
        ).map(WgsCramService.v1QueryConverter.buildQuery(_)(ElasticsearchIndex.WgsCram))
      )
    }

    Post(
      "/metadata/GCP/proj0/alias/2",
      Map(
        "cram_md5" -> "abcgithashdef",
        "notes" -> "some note",
        "document_status" -> "Deleted",
        "cram_path" -> ""
      )
    ) ~> webService.cramPostMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 2
      val secondUpdate = memorySearchDAO.updateCalls
        .flatMap(_._1.headOption)
        .apply(1)

      getStringByName(secondUpdate, "cram_md5") should be("abcgithashdef")
      getStringByName(secondUpdate, "notes") should be("some note")
      getDocumentStatus(secondUpdate) should be(DocumentStatus.Deleted)
      getUriByName(secondUpdate, "cram_path") should be(URI.create(""))
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/queryall", Map("location" -> "GCP")) ~> webService.cramQueryall ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          // From /query call earlier in the test.
          TransferWgsCramV1QueryInput(
            location = Some(Location.GCP),
            documentStatus = Some(DocumentStatus.Normal)
          ),
          // No documentStatus restriction from /queryall
          TransferWgsCramV1QueryInput(location = Some(Location.GCP))
        ).map(WgsCramService.v1QueryConverter.buildQuery(_)(ElasticsearchIndex.WgsCram))
      )
    }
  }

  it should "query with a BoGuS project and sample and return nothing" in {
    val webService = new MockWgsCramWebService()
    Post(
      "/query",
      Map("project" -> "testBoGuSproject", "sample_alias" -> "testBoGuSsample")
    ) ~> webService.cramQuery ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "reject postMetadata with BoGuS location" in {
    val webService = new MockWgsCramWebService()
    Post("/metadata/BoGuS/proj0/alias/2", Map("project" -> "testBoGuSlocation")) ~> Route
      .seal(webService.cramPostMetadata) ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "query with an empty request" in {
    val webService = new MockWgsCramWebService()
    Post("/query", Map.empty[String, String]) ~> webService.cramQuery ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "query without an empty request" in {
    val webService = new MockWgsCramWebService()
    Post("/query", Map("project" -> "testProject")) ~> webService.cramQuery ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "return a JSON schema" in {
    val webService = new MockWgsCramWebService()
    Get("/schema") ~> webService.cramGetSchema ~> check {
      responseAs[Json] should be(WgsCramIndex.jsonSchema)
    }
  }
}
