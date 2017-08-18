package org.broadinstitute.clio.integrationtest.tests

import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.{TransferReadGroupV1Key, TransferReadGroupV1Metadata, TransferReadGroupV1QueryInput, TransferReadGroupV1QueryOutput}
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json

import scala.concurrent.Future

/** Tests of Clio's read-group / uBAM functionality. */
trait ReadGroupTests { self: BaseIntegrationSpec =>

  it should "create the expected read-group mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    val expected = ElasticsearchIndex.ReadGroup

    val getRequest =
      getMapping(IndexAndType(expected.indexName, expected.indexType))
    elasticsearchClient.execute(getRequest).map { mappings =>
      mappings should have length 1
      val readGroupMapping = mappings.head

      readGroupMapping should be(indexToMapping(expected))
    }
  }

  it should "report the expected JSON schema for read-groups" in {
    clioWebClient
      .getReadGroupSchema(bearerToken)
      .flatMap(Unmarshal(_).to[Json])
      .map(_ should be(JsonSchemas.ReadGroup))
  }

  // Generate a test for every possible Location value.
  Location.values.foreach {
    it should behave like testReadGroupLocation(_)
  }

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testReadGroupLocation(location: Location): Unit = {
    val expected = TransferReadGroupV1QueryOutput(
      flowcellBarcode = "barcode2",
      lane = 2,
      libraryName = s"library$randomId",
      location = location,
      project = Some("testProject")
    )

    val upsertKey = TransferReadGroupV1Key(
      expected.flowcellBarcode,
      expected.lane,
      expected.libraryName,
      location
    )
    val upsertData = TransferReadGroupV1Metadata(project = expected.project)

    if (location == Location.Unknown) {

      it should "reject read-group inputs with unknown location" in {
        clioWebClient
          .addReadGroupBam(bearerToken, upsertKey, upsertData)
          .map(_.status should be(StatusCodes.NotFound))
      }

    } else {
      val queryData = TransferReadGroupV1QueryInput(
        libraryName = Some(expected.libraryName)
      )

      it should s"handle upserts and queries for read-group location $location" in {
        for {
          _ <- clioWebClient
            .addReadGroupBam(bearerToken, upsertKey, upsertData)
            .map(_.status should be(StatusCodes.OK))
          queryResponse <- clioWebClient.queryReadGroupBam(
            bearerToken,
            queryData
          )
          outputs <- Unmarshal(queryResponse)
            .to[Seq[TransferReadGroupV1QueryOutput]]
        } yield {
          outputs should have length 1
          outputs.head should be(expected)
        }
      }
    }
  }

  it should "handle querying read-groups by sample and project" in {
    val flowcellBarcode = "barcode2"
    val lane = 2
    val location = Location.GCP
    val project = "testProject" + randomId

    val libraries = Seq.fill(3)("library" + randomId)
    val samples = {
      val sameId = "testSample" + randomId
      Seq(sameId, sameId, "testSample" + randomId)
    }

    val upserts = Future.sequence {
      libraries.zip(samples).map {
        case (library, sample) =>
          val key =
            TransferReadGroupV1Key(flowcellBarcode, lane, library, location)
          val data = TransferReadGroupV1Metadata(
            project = Some(project),
            sampleAlias = Some(sample)
          )
          clioWebClient.addReadGroupBam(bearerToken, key, data)
      }
    }

    val upsertsStatus = upserts.flatMap { requests =>
      requests.foldLeft(succeed) { (_, req) =>
        req.status should be(StatusCodes.OK)
      }
    }

    for {
      _ <- upsertsStatus
      queryProject = TransferReadGroupV1QueryInput(project = Some(project))
      querySample = TransferReadGroupV1QueryInput(
        sampleAlias = Some(samples.head)
      )
      projectResponse <- clioWebClient.queryReadGroupBam(
        bearerToken,
        queryProject
      )
      projectResults <- Unmarshal(projectResponse)
        .to[Seq[TransferReadGroupV1QueryOutput]]
      sampleResponse <- clioWebClient.queryReadGroupBam(
        bearerToken,
        querySample
      )
      sampleResults <- Unmarshal(sampleResponse)
        .to[Seq[TransferReadGroupV1QueryOutput]]
    } yield {
      projectResults should have length 3
      projectResults.foldLeft(succeed) { (_, result) =>
        result.project should be(Some(project))
      }
      sampleResults should have length 2
      sampleResults.foldLeft(succeed) { (_, result) =>
        result.sampleAlias should be(Some(samples.head))
      }
    }
  }

  it should "handle updates to read-group metadata" in {
    val flowcellBarcode = "barcode2"
    val lane = 2
    val libraryName = "library" + randomId
    val location = Location.GCP
    val metadata = TransferReadGroupV1Metadata(
      project = Some("testProject" + randomId),
      sampleAlias = Some("sampleAlias1"),
      notes = Some("Breaking news")
    )

    val upsertKey =
      TransferReadGroupV1Key(flowcellBarcode, lane, libraryName, location)
    val upsertData = TransferReadGroupV1Metadata(
      sampleAlias = metadata.sampleAlias,
      project = metadata.project
    )

    val queryData = TransferReadGroupV1QueryInput(project = metadata.project)

    def add(data: TransferReadGroupV1Metadata) =
      clioWebClient
        .addReadGroupBam(bearerToken, upsertKey, data)
        .map(_.status should be(StatusCodes.OK))

    def query =
      for {
        response <- clioWebClient.queryReadGroupBam(bearerToken, queryData)
        results <- Unmarshal(response).to[Seq[TransferReadGroupV1QueryOutput]]
      } yield {
        results should have length 1
        results.head
      }

    for {
      _ <- add(upsertData)
      original <- query
      _ = original.sampleAlias should be(metadata.sampleAlias)
      _ = original.notes should be(None)
      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- add(upsertData2)
      withNotes <- query
      _ = withNotes.sampleAlias should be(metadata.sampleAlias)
      _ = withNotes.notes should be(metadata.notes)
      _ <- add(
        upsertData2.copy(sampleAlias = Some("sampleAlias2"), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.sampleAlias should be(Some("sampleAlias2"))
      emptyNotes.notes should be(Some(""))
    }
  }

  it should "show deleted records on queryAll, but not query" in {
    val barcode = "fc5440"
    val project = "testProject" + randomId
    val sample = "sample688." + randomId
    val keysWithMetadata = (0 until 3).map { lane =>
      val upsertKey = TransferReadGroupV1Key(
        flowcellBarcode = barcode,
        lane = lane,
        libraryName = "library" + randomId,
        location = Location.GCP
      )
      val upsertMetadata = TransferReadGroupV1Metadata(
        project = Some(project),
        sampleAlias = Some(sample),
        ubamPath = Some(s"gs://ubam/$sample.$lane")
      )
      (upsertKey, upsertMetadata)
    }
    val (deleteKey, deleteData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (key, metadata) =>
          clioWebClient.addReadGroupBam(bearerToken, key, metadata)
      }
    }

    val upsertsStatus = upserts.flatMap { requests =>
      requests.foldLeft(succeed) { (_, req) =>
        req.status should be(StatusCodes.OK)
      }
    }

    val queryData = TransferReadGroupV1QueryInput(
      project = Some(project),
      flowcellBarcode = Some(barcode)
    )
    def checkQuery(expectedLength: Int) = {
      for {
        response <- clioWebClient.queryReadGroupBam(bearerToken, queryData)
        results <- Unmarshal(response).to[Seq[TransferReadGroupV1QueryOutput]]
      } yield {
        results.length should be(expectedLength)
        results.foreach { result =>
          result.project should be(Some(project))
          result.sampleAlias should be(Some(sample))
          result.documentStatus should be(Some(DocumentStatus.Normal))
        }
        results
      }
    }

    for {
      _ <- upsertsStatus
      _ <- checkQuery(expectedLength = 3)
      // TODO: Weird to use the `addReadGroupBam` method here,
      // even though it works.
      deleteResponse <- clioWebClient.addReadGroupBam(
        bearerToken,
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ = deleteResponse.status should be(StatusCodes.OK)
      _ <- checkQuery(expectedLength = 2)

      // Client doesn't currently have a method for queryAll.
      entity <- Marshal(queryData).to[RequestEntity]
      request = HttpRequest(
        uri = s"/api/v1/readgroup/queryAll",
        method = HttpMethods.POST,
        entity = entity
      )
      response <- clioWebClient.dispatchRequest(request)
      results <- Unmarshal(response).to[Seq[TransferReadGroupV1QueryOutput]]
    } yield {
      results.length should be(keysWithMetadata.length)
      results.foldLeft(succeed) { (_, result) =>
        result.project should be(Some(project))
        result.sampleAlias should be(Some(sample))

        val resultKey = TransferReadGroupV1Key(
          flowcellBarcode = result.flowcellBarcode,
          lane = result.lane,
          libraryName = result.libraryName,
          location = result.location
        )

        if (resultKey == deleteKey) {
          result.documentStatus should be(Some(DocumentStatus.Normal))
        } else {
          result.documentStatus should be(Some(DocumentStatus.Deleted))
        }
      }
    }
  }
}
