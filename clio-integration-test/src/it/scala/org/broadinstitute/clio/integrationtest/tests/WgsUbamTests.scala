package org.broadinstitute.clio.integrationtest.tests

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.fasterxml.uuid.impl.{TimeBasedGenerator, UUIDUtil}
import com.fasterxml.uuid.{EthernetAddress, Generators}
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.Future

/** Tests of Clio's wgs-ubam functionality. */
trait WgsUbamTests { self: BaseIntegrationSpec =>

  it should "create the expected wgs-ubam mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val expected = ElasticsearchIndex.WgsUbam
    val getRequest =
      getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.execute(getRequest).map { mappings =>
      mappings should have length 1
      val wgsUbamMapping = mappings.head
      wgsUbamMapping should be(indexToMapping(expected))
    }
  }

  it should "report the expected JSON schema for wgs-ubams" in {
    clioWebClient.getWgsUbamSchema
      .flatMap(Unmarshal(_).to[Json])
      .map(_ should be(JsonSchemas.WgsUbam))
  }

  // Generate a test for every possible Location value.
  Location.values.foreach {
    it should behave like testWgsUbamLocation(_)
  }

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testWgsUbamLocation(location: Location): Unit = {
    val expected = TransferWgsUbamV1QueryOutput(
      flowcellBarcode = "barcode2",
      lane = 2,
      libraryName = s"library$randomId",
      location = location,
      project = Some("testProject"),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val upsertKey = TransferWgsUbamV1Key(
      expected.flowcellBarcode,
      expected.lane,
      expected.libraryName,
      location
    )
    val upsertData = TransferWgsUbamV1Metadata(project = expected.project)

    if (location == Location.Unknown) {
      it should "reject wgs-ubam inputs with unknown location" in {
        clioWebClient
          .addWgsUbam(upsertKey, upsertData)
          .map(_.status should be(StatusCodes.NotFound))
      }
    } else {
      val queryData = TransferWgsUbamV1QueryInput(
        libraryName = Some(expected.libraryName)
      )

      it should s"handle upserts and queries for wgs-ubam location $location" in {
        for {
          upsertResponse <- clioWebClient
            .addWgsUbam(upsertKey, upsertData)
          returnedClioId <- Unmarshal(upsertResponse).to[String]
          queryResponse <- clioWebClient
            .queryWgsUbam(queryData)
          outputs <- Unmarshal(queryResponse)
            .to[Seq[TransferWgsUbamV1QueryOutput]]
        } yield {
          upsertResponse.status should be(StatusCodes.OK)
          outputs should have length 1
          outputs.head should be(
            expected.copy(clioId = Some(UUIDUtil.uuid(returnedClioId)))
          )
        }
      }
    }
  }

  it should "assign different clioIds to different WgsUbam upserts" in {
    val upsertKey = TransferWgsUbamV1Key(
      flowcellBarcode = "testClioIdBarcode",
      lane = 2,
      libraryName = s"library$randomId",
      location = Location.GCP,
    )
    val upsertData = TransferWgsUbamV1Metadata(project = Some("testProject1"))

    for {
      clioId1 <- clioWebClient
        .addWgsUbam(upsertKey, upsertData)
        .flatMap(Unmarshal(_).to[String])
      clioId2 <- clioWebClient
        .addWgsUbam(upsertKey, upsertData.copy(project = Some("testProject2")))
        .flatMap(Unmarshal(_).to[String])
    } yield {
      UUIDUtil.uuid(clioId2).compareTo(UUIDUtil.uuid(clioId1)) should be(1)
    }
  }

  it should "assign different clioIds to equal WgsUbam upserts" in {
    val upsertKey = TransferWgsUbamV1Key(
      flowcellBarcode = "testClioIdBarcode",
      lane = 2,
      libraryName = s"library$randomId",
      location = Location.GCP,
    )
    val upsertData = TransferWgsUbamV1Metadata(project = Some("testProject1"))

    for {
      clioId1 <- clioWebClient
        .addWgsUbam(upsertKey, upsertData)
        .flatMap(Unmarshal(_).to[String])
      clioId2 <- clioWebClient
        .addWgsUbam(upsertKey, upsertData)
        .flatMap(Unmarshal(_).to[String])
    } yield {
      UUIDUtil.uuid(clioId2).compareTo(UUIDUtil.uuid(clioId1)) should be(1)
    }
  }

  it should "decode clioIds and supply a new one on upserts" in {
    val generator: TimeBasedGenerator =
      Generators.timeBasedGenerator(EthernetAddress.fromInterface())
    val oldClioId = generator.generate()

    val upsertKey = TransferWgsUbamV1Key(
      flowcellBarcode = "testClioIdBarcode",
      lane = 2,
      libraryName = s"library$randomId",
      location = Location.GCP,
    )
    val upsertData = TransferWgsUbamV1Metadata(clioId = Some(oldClioId))

    for {
      response <- clioWebClient
        .addWgsUbam(upsertKey, upsertData)
      newClioId <- Unmarshal(response).to[String]
    } yield {
      response.status should be(StatusCodes.OK)
      val newUuid = UUIDUtil.uuid(newClioId)
      newUuid should not equal oldClioId
      newUuid.compareTo(oldClioId) should be(1)
    }
  }

  it should "handle querying wgs-ubams by sample and project" in {
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
            TransferWgsUbamV1Key(flowcellBarcode, lane, library, location)
          val data = TransferWgsUbamV1Metadata(
            project = Some(project),
            sampleAlias = Some(sample)
          )
          clioWebClient.addWgsUbam(key, data)
      }
    }

    val upsertsStatus = upserts.flatMap { requests =>
      requests.foldLeft(succeed) { (_, req) =>
        req.status should be(StatusCodes.OK)
      }
    }

    for {
      _ <- upsertsStatus
      queryProject = TransferWgsUbamV1QueryInput(project = Some(project))
      querySample = TransferWgsUbamV1QueryInput(
        sampleAlias = Some(samples.head)
      )
      projectResponse <- clioWebClient.queryWgsUbam(queryProject)
      projectResults <- Unmarshal(projectResponse)
        .to[Seq[TransferWgsUbamV1QueryOutput]]
      sampleResponse <- clioWebClient.queryWgsUbam(querySample)
      sampleResults <- Unmarshal(sampleResponse)
        .to[Seq[TransferWgsUbamV1QueryOutput]]
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

  it should "handle updates to wgs-ubam metadata" in {
    val flowcellBarcode = "barcode2"
    val lane = 2
    val libraryName = "library" + randomId
    val location = Location.GCP
    val metadata = TransferWgsUbamV1Metadata(
      project = Some("testProject" + randomId),
      sampleAlias = Some("sampleAlias1"),
      notes = Some("Breaking news")
    )

    val upsertKey =
      TransferWgsUbamV1Key(flowcellBarcode, lane, libraryName, location)
    val upsertData = TransferWgsUbamV1Metadata(
      sampleAlias = metadata.sampleAlias,
      project = metadata.project
    )
    val queryData = TransferWgsUbamV1QueryInput(project = metadata.project)

    def add(data: TransferWgsUbamV1Metadata) = {
      clioWebClient
        .addWgsUbam(upsertKey, data)
        .map(_.status should be(StatusCodes.OK))
    }

    def query = {
      for {
        response <- clioWebClient.queryWgsUbam(queryData)
        results <- Unmarshal(response).to[Seq[TransferWgsUbamV1QueryOutput]]
      } yield {
        results should have length 1
        results.head
      }
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
    val keysWithMetadata = (1 to 3).map { lane =>
      val upsertKey = TransferWgsUbamV1Key(
        flowcellBarcode = barcode,
        lane = lane,
        libraryName = "library" + randomId,
        location = Location.GCP
      )
      val upsertMetadata = TransferWgsUbamV1Metadata(
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
          clioWebClient.addWgsUbam(key, metadata)
      }
    }

    val upsertsStatus = upserts.flatMap { requests =>
      requests.foldLeft(succeed) { (_, req) =>
        req.status should be(StatusCodes.OK)
      }
    }

    val queryData = TransferWgsUbamV1QueryInput(
      project = Some(project),
      flowcellBarcode = Some(barcode)
    )

    def checkQuery(expectedLength: Int) = {
      for {
        response <- clioWebClient.queryWgsUbam(queryData)
        results <- Unmarshal(response).to[Seq[TransferWgsUbamV1QueryOutput]]
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
      // TODO: Weird to use the `addWgsUbam` method here, even though it works.
      deleteResponse <- clioWebClient.addWgsUbam(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ = deleteResponse.status should be(StatusCodes.OK)
      _ <- checkQuery(expectedLength = 2)

      // Client doesn't currently have a method for queryAll.
      entity <- Marshal(queryData).to[RequestEntity]
      request = HttpRequest(
        uri = s"/api/v1/wgsubam/queryall",
        method = HttpMethods.POST,
        entity = entity
      ).addCredentials(bearerToken)
      response <- clioWebClient.dispatchRequest(request)
      results <- Unmarshal(response).to[Seq[TransferWgsUbamV1QueryOutput]]
    } yield {
      results.length should be(keysWithMetadata.length)
      results.foldLeft(succeed) { (_, result) =>
        result.project should be(Some(project))
        result.sampleAlias should be(Some(sample))

        val resultKey = TransferWgsUbamV1Key(
          flowcellBarcode = result.flowcellBarcode,
          lane = result.lane,
          libraryName = result.libraryName,
          location = result.location
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
