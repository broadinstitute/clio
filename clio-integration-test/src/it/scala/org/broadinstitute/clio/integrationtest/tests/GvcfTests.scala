package org.broadinstitute.clio.integrationtest.tests

import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentGvcf,
  ElasticsearchIndex
}
import org.broadinstitute.clio.transfer.model.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput,
  TransferGvcfV1QueryOutput
}
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{
  HttpMethods,
  HttpRequest,
  RequestEntity,
  StatusCodes
}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json

import scala.concurrent.Future
import java.util.UUID
import com.sksamuel.elastic4s.http.ElasticDsl._

/** Tests of Clio's gvcf functionality. */
trait GvcfTests { self: BaseIntegrationSpec =>

  it should "create the expected gvcf mapping in elasticsearch" in {

    val expected = ElasticsearchIndex.Gvcf
    val getRequest =
      getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.execute(getRequest).map { mappings =>
      mappings should have length 1
      val gvcfMapping = mappings.head
      gvcfMapping should be(indexToMapping(expected))
    }
  }

  it should "report the expected JSON schema for gvcf" in {
    clioWebClient
      .getSchemaGvcf(bearerToken)
      .flatMap(Unmarshal(_).to[Json])
      .map(_ should be(JsonSchemas.Gvcf))
  }

  // Generate a test for every possible Location value.
  Location.values.foreach {
    it should behave like testGvcfLocation(_)
  }

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testGvcfLocation(location: Location): Unit = {
    val expected = TransferGvcfV1QueryOutput(
      location = location,
      project = "testProject",
      sampleAlias = "someAlias",
      version = 2,
      documentStatus = Some(DocumentStatus.Normal),
      gvcfPath = Some("gs://path/gvcf.gvcf")
    )

    val upsertKey = TransferGvcfV1Key(
      location,
      expected.project,
      expected.sampleAlias,
      expected.version
    )
    val upsertData = TransferGvcfV1Metadata(
      gvcfPath = Some("gs://path/gvcf.gvcf")
    )

    if (location == Location.Unknown) {
      it should "reject gvcf inputs with unknown location" in {
        clioWebClient
          .addGvcf(upsertKey, upsertData)
          .map(_.status should be(StatusCodes.NotFound))
      }
    } else {
      val queryData = TransferGvcfV1QueryInput(
        location = Some(expected.location)
      )

      it should s"handle upserts and queries for gvcf location $location" in {
        for {
          upsertResponse <- clioWebClient
            .addGvcf(upsertKey, upsertData)
          returnedClioId <- Unmarshal(upsertResponse).to[UUID]
          queryResponse <- clioWebClient
            .queryGvcf(queryData)
          outputs <- Unmarshal(queryResponse)
            .to[Seq[TransferGvcfV1QueryOutput]]
        } yield {
          upsertResponse.status should be(StatusCodes.OK)
          outputs should have length 1
          outputs.head should be(expected)

          val storedDocument =
            getJsonFrom[DocumentGvcf](ElasticsearchIndex.Gvcf, returnedClioId)
          storedDocument.location should be(upsertKey.location)
          storedDocument.project should be(upsertKey.project)
          storedDocument.sampleAlias should be(upsertKey.sampleAlias)
          storedDocument.version should be(upsertKey.version)
          storedDocument.gvcfPath should be(upsertData.gvcfPath)
        }
      }
    }
  }

  it should "assign different clioIds to different gvcf upserts" in {
    val upsertKey = TransferGvcfV1Key(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )
    val upsertData =
      TransferGvcfV1Metadata(gvcfPath = Some("gs://path/gvcf1.gvcf"))

    for {
      clioId1 <- clioWebClient
        .addGvcf(upsertKey, upsertData)
        .flatMap(Unmarshal(_).to[UUID])
      clioId2 <- clioWebClient
        .addGvcf(
          upsertKey,
          upsertData.copy(gvcfPath = Some("gs://path/gvcf2.gvcf"))
        )
        .flatMap(Unmarshal(_).to[UUID])
    } yield {
      clioId2.compareTo(clioId1) should be(1)

      val storedDocument1 =
        getJsonFrom[DocumentGvcf](ElasticsearchIndex.Gvcf, clioId1)
      storedDocument1.gvcfPath should be(Some("gs://path/gvcf1.gvcf"))

      val storedDocument2 =
        getJsonFrom[DocumentGvcf](ElasticsearchIndex.Gvcf, clioId2)
      storedDocument2.gvcfPath should be(Some("gs://path/gvcf2.gvcf"))

      storedDocument1.copy(
        clioId = clioId2,
        gvcfPath = Some("gs://path/gvcf2.gvcf")
      ) should be(storedDocument2)
    }
  }

  it should "assign different clioIds to equal gvcf upserts" in {
    val upsertKey = TransferGvcfV1Key(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )
    val upsertData =
      TransferGvcfV1Metadata(gvcfPath = Some("gs://path/gvcf1.gvcf"))

    for {
      clioId1 <- clioWebClient
        .addGvcf(upsertKey, upsertData)
        .flatMap(Unmarshal(_).to[UUID])
      clioId2 <- clioWebClient
        .addGvcf(upsertKey, upsertData)
        .flatMap(Unmarshal(_).to[UUID])
    } yield {
      clioId2.compareTo(clioId1) should be(1)

      val storedDocument1 =
        getJsonFrom[DocumentGvcf](ElasticsearchIndex.Gvcf, clioId1)
      val storedDocument2 =
        getJsonFrom[DocumentGvcf](ElasticsearchIndex.Gvcf, clioId2)
      storedDocument1.copy(clioId = clioId2) should be(storedDocument2)
    }
  }

  it should "handle querying gvcf by sample and project" in {
    val location = Location.GCP
    val project = "testProject" + randomId

    val samples = {
      val sameId = "testSample" + randomId
      Seq(sameId, sameId, "testSample" + randomId)
    }

    val upserts = Future.sequence {
      samples.zip(1 to 3).map {
        case (sample, version) =>
          val key = TransferGvcfV1Key(location, project, sample, version)
          val data = TransferGvcfV1Metadata(
            gvcfPath = Some("gs://path/gvcf.gvcf"),
            contamination = Some(.65f)
          )
          clioWebClient.addGvcf(key, data)
      }
    }

    val upsertsStatus = upserts.flatMap { requests =>
      requests.foldLeft(succeed) { (_, req) =>
        req.status should be(StatusCodes.OK)
      }
    }

    for {
      _ <- upsertsStatus
      queryProject = TransferGvcfV1QueryInput(project = Some(project))
      querySample = TransferGvcfV1QueryInput(sampleAlias = Some(samples.head))
      projectResponse <- clioWebClient.queryGvcf(queryProject)
      projectResults <- Unmarshal(projectResponse)
        .to[Seq[TransferGvcfV1QueryOutput]]
      sampleResponse <- clioWebClient.queryGvcf(querySample)
      sampleResults <- Unmarshal(sampleResponse)
        .to[Seq[TransferGvcfV1QueryOutput]]
    } yield {
      projectResults should have length 3
      projectResults.foldLeft(succeed) { (_, result) =>
        result.project should be(project)
      }
      sampleResults should have length 2
      sampleResults.foldLeft(succeed) { (_, result) =>
        result.sampleAlias should be(samples.head)
      }
    }
  }

  it should "handle updates to gvcf metadata" in {
    val location = Location.GCP
    val metadata = TransferGvcfV1Metadata(
      gvcfPath = Some("gs://path/gvcf.gvcf"),
      contamination = Some(.75f),
      notes = Some("Breaking news")
    )
    val project = "testProject" + randomId
    val version = 1
    val sampleAlias = "testSample" + randomId

    val upsertKey =
      TransferGvcfV1Key(location, project, sampleAlias, version)
    val upsertData = TransferGvcfV1Metadata(
      contamination = metadata.contamination,
      gvcfPath = metadata.gvcfPath
    )
    val queryData = TransferGvcfV1QueryInput(project = Some(project))

    def add(data: TransferGvcfV1Metadata) = {
      clioWebClient
        .addGvcf(upsertKey, data)
        .map(_.status should be(StatusCodes.OK))
    }

    def query = {
      for {
        response <- clioWebClient.queryGvcf(queryData)
        results <- Unmarshal(response).to[Seq[TransferGvcfV1QueryOutput]]
      } yield {
        results should have length 1
        results.head
      }
    }

    for {
      _ <- add(upsertData)
      original <- query
      _ = original.gvcfPath should be(metadata.gvcfPath)
      _ = original.notes should be(None)
      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- add(upsertData2)
      withNotes <- query
      _ = withNotes.gvcfPath should be(metadata.gvcfPath)
      _ = withNotes.notes should be(metadata.notes)
      _ <- add(
        upsertData2
          .copy(gvcfPath = Some("gs://path2/gvcf.gvcf"), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.gvcfPath should be(Some("gs://path2/gvcf.gvcf"))
      emptyNotes.notes should be(Some(""))
    }
  }

  it should "show deleted gvcf records on queryAll, but not query" in {
    val project = "testProject" + randomId
    val sampleAlias = "sample688." + randomId

    val keysWithMetadata = (1 to 3).map { version =>
      val upsertKey = TransferGvcfV1Key(
        location = Location.GCP,
        project = project,
        sampleAlias = sampleAlias,
        version = version
      )
      val upsertMetadata = TransferGvcfV1Metadata(
        gvcfPath = Some(s"gs://gvcf/$sampleAlias.$version")
      )
      (upsertKey, upsertMetadata)
    }
    val (deleteKey, deleteData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (key, metadata) =>
          clioWebClient.addGvcf(key, metadata)
      }
    }

    val upsertsStatus = upserts.flatMap { requests =>
      requests.foldLeft(succeed) { (_, req) =>
        req.status should be(StatusCodes.OK)
      }
    }

    val queryData = TransferGvcfV1QueryInput(
      location = Some(Location.GCP),
      project = Some(project)
    )

    def checkQuery(expectedLength: Int) = {
      for {
        response <- clioWebClient.queryGvcf(queryData)
        results <- Unmarshal(response).to[Seq[TransferGvcfV1QueryOutput]]
      } yield {
        results.length should be(expectedLength)
        results.foreach { result =>
          result.project should be(project)
          result.sampleAlias should be(sampleAlias)
          result.documentStatus should be(Some(DocumentStatus.Normal))
        }
        results
      }
    }

    for {
      _ <- upsertsStatus
      _ <- checkQuery(expectedLength = 3)
      // TODO: Weird to use the `addGvcf` method here, even though it works.
      deleteResponse <- clioWebClient.addGvcf(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ = deleteResponse.status should be(StatusCodes.OK)
      _ <- checkQuery(expectedLength = 2)

      // Client doesn't currently have a method for queryAll.
      entity <- Marshal(queryData).to[RequestEntity]
      request = HttpRequest(
        uri = s"/api/v1/gvcf/queryall",
        method = HttpMethods.POST,
        entity = entity
      ).addCredentials(bearerToken)
      response <- clioWebClient.dispatchRequest(request)
      results <- Unmarshal(response).to[Seq[TransferGvcfV1QueryOutput]]
    } yield {
      results.length should be(keysWithMetadata.length)
      results.foldLeft(succeed) { (_, result) =>
        result.project should be(project)
        result.sampleAlias should be(sampleAlias)

        val resultKey = TransferGvcfV1Key(
          location = result.location,
          project = result.project,
          sampleAlias = result.sampleAlias,
          version = result.version
        )

        if (resultKey == deleteKey) {
          result.documentStatus should be(Some(DocumentStatus.Deleted))
        } else {
          result.documentStatus should be(Some(DocumentStatus.Normal))
        }
      }
    }
  }
}
