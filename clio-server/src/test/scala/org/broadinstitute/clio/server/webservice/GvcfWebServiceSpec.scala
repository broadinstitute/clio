package org.broadinstitute.clio.server.webservice

import java.net.URI

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import io.circe.Json
import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.MemorySearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  AutoElasticsearchQueryMapper,
  DocumentGvcf
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  TransferGvcfV1QueryInput,
  TransferGvcfV1QueryOutput
}

class GvcfWebServiceSpec extends BaseWebserviceSpec {
  behavior of "GvcfWebService"

  private val mockQueryConverter = AutoElasticsearchQueryMapper[
    TransferGvcfV1QueryInput,
    TransferGvcfV1QueryOutput,
    DocumentGvcf
  ]

  it should "postMetadata with OnPrem location" in {
    val webService = new MockGvcfWebService()
    Post("/metadata/OnPrem/proj0/sample_alias0/1", Map("notes" -> "some note")) ~> webService.postMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
  }

  it should "postMetadata with GCP location" in {
    val webService = new MockGvcfWebService()
    Post("/metadata/GCP/proj0/sample_alias0/1", Map("notes" -> "some note")) ~> webService.postMetadata ~> check {
      UpsertId.isValidId(responseAs[String]) should be(true)
    }
  }

  it should "query with a project and sample and return multiple records" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val webService = new MockGvcfWebService(app)
    Post(
      "/metadata/OnPrem/proj0/sample_alias0/1",
      Map(
        "notes" -> "some note",
        "gvcf_path" -> s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"
      )
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/OnPrem/proj1/sample_alias0/1",
      Map(
        "notes" -> "some note",
        "gvcf_path" -> s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"
      )
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(
      "/metadata/OnPrem/proj2/sample_alias0/1",
      Map(
        "notes" -> "some note",
        "gvcf_path" -> s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"
      )
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("project" -> "proj0")) ~> webService.query ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferGvcfV1QueryInput(
            project = Some("proj0"),
            documentStatus = Some(DocumentStatus.Normal)
          )
        ).map(mockQueryConverter.buildQuery)
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
    val webService = new MockGvcfWebService(app)
    Post(
      "/metadata/GCP/proj1/sample_alias0/1",
      Map(
        "gvcf_md5" -> "abcgithashdef",
        "notes" -> "some note",
        "gvcf_path" -> s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"
      )
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 1
      val firstUpdate = memorySearchDAO.updateCalls.headOption
        .flatMap(_._1.headOption)
        .getOrElse {
          // Doing this .headOption.getOrElse dance because Codacy
          // scolds us for using .head
          fail("Impossible because of the above check")
        }
        .as[DocumentGvcf]
        .fold(throw _, identity)

      firstUpdate.upsertId should be(responseAs[UpsertId])
      firstUpdate.gvcfMd5 should be(Some('abcgithashdef))
      firstUpdate.notes should be(Some("some note"))
      firstUpdate.gvcfPath should be(
        Some(URI.create(s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"))
      )
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/query", Map("location" -> "GCP")) ~> webService.query ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          TransferGvcfV1QueryInput(
            location = Some(Location.GCP),
            documentStatus = Some(DocumentStatus.Normal)
          )
        ).map(mockQueryConverter.buildQuery)
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
    ) ~> webService.postMetadata ~> check {
      status shouldEqual StatusCodes.OK
      memorySearchDAO.updateCalls should have length 2
      val secondUpdate = memorySearchDAO.updateCalls
        .flatMap(_._1.headOption)
        .apply(1)
        .as[DocumentGvcf]
        .fold(throw _, identity)

      secondUpdate.gvcfMd5 should be(Some('abcgithashdef))
      secondUpdate.notes should be(Some("some note"))
      secondUpdate.documentStatus should be(Some(DocumentStatus.Deleted))
      secondUpdate.gvcfPath should be(Some(URI.create("")))
    }

    // We have to test the MemorySearchDAO because we're not going to implement
    // Elasticsearch logic in our test specs. Here, we're just verifying that
    // the web service passes the appropriate queries onto the search DAO.
    Post("/queryall", Map("location" -> "GCP")) ~> webService.queryall ~> check {
      memorySearchDAO.queryCalls should be(
        Seq(
          // From /query call earlier in the test.
          TransferGvcfV1QueryInput(
            location = Some(Location.GCP),
            documentStatus = Some(DocumentStatus.Normal)
          ),
          // No documentStatus restriction from /queryall
          TransferGvcfV1QueryInput(location = Some(Location.GCP))
        ).map(mockQueryConverter.buildQuery)
      )
    }
  }

  it should "query with a BoGuS project and sample and return nothing" in {
    val webService = new MockGvcfWebService()
    Post(
      "/query",
      Map("project" -> "testBoGuSproject", "sample_alias" -> "testBoGuSsample")
    ) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "reject postMetadata with BoGuS location" in {
    val webService = new MockGvcfWebService()
    Post("/metadata/BoGuS/proj0/alias/2", Map("project" -> "testBoGuSlocation")) ~> Route
      .seal(webService.postMetadata) ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "query with an empty request" in {
    val webService = new MockGvcfWebService()
    Post("/query", Map.empty[String, String]) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "query without an empty request" in {
    val webService = new MockGvcfWebService()
    Post("/query", Map("project" -> "testProject")) ~> webService.query ~> check {
      responseAs[Seq[String]] should be(empty)
    }
  }

  it should "return a JSON schema" in {
    val webService = new MockGvcfWebService()
    Get("/schema") ~> webService.getSchema ~> check {
      responseAs[Json] should be(GvcfIndex.jsonSchema)
    }
  }
}
