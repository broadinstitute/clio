package org.broadinstitute.clio.integrationtest.tests

import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentWgsUbam,
  ElasticsearchIndex
}
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import akka.http.scaladsl.unmarshalling.Unmarshal
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json

import scala.concurrent.Future

import java.util.UUID

/** Tests of Clio's wgs-ubam functionality. */
trait WgsUbamTests { self: BaseIntegrationSpec =>

  def runUpsert(key: TransferWgsUbamV1Key,
                metadata: TransferWgsUbamV1Metadata): Future[UUID] = {
    val tmpMetadata = writeTmpJson(metadata)
    runClient(
      WgsUbamTests.upsert,
      "--flowcell-barcode",
      key.flowcellBarcode,
      "--lane",
      key.lane.toString,
      "--library-name",
      key.libraryName,
      "--location",
      key.location.entryName,
      "--metadata-location",
      tmpMetadata.toString
    ).flatMap(Unmarshal(_).to[UUID])
  }

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
    runClient("get-wgs-ubam-schema")
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

    /*
     * NOTE: This is lazy on purpose. If it executes outside of the actual `it` block,
     * it'll result in an `UninitializedFieldError` because the spec `beforeAll` won't
     * have triggered yet.
     */
    lazy val responseFuture = runUpsert(
      TransferWgsUbamV1Key(
        expected.flowcellBarcode,
        expected.lane,
        expected.libraryName,
        expected.location
      ),
      TransferWgsUbamV1Metadata(project = expected.project)
    )

    if (location == Location.Unknown) {
      it should "reject wgs-ubam inputs with unknown location" in {
        recoverToSucceededIf[Exception](responseFuture)
      }
    } else {
      it should s"handle upserts and queries for wgs-ubam location $location" in {
        for {
          upsertResponse <- responseFuture
          returnedClioId <- Unmarshal(upsertResponse).to[UUID]
          queryResponse <- runClient(
            WgsUbamTests.query,
            "--library-name",
            expected.libraryName
          )
          outputs <- Unmarshal(queryResponse)
            .to[Seq[TransferWgsUbamV1QueryOutput]]
        } yield {
          outputs should have length 1
          outputs.head should be(expected)

          val storedDocument = getJsonFrom[DocumentWgsUbam](
            ElasticsearchIndex.WgsUbam,
            returnedClioId
          )
          storedDocument.flowcellBarcode should be(expected.flowcellBarcode)
          storedDocument.lane should be(expected.lane)
          storedDocument.libraryName should be(expected.libraryName)
          storedDocument.location should be(expected.location)
          storedDocument.project should be(expected.project)
        }
      }
    }
  }

  it should "assign different clioIds to different wgs-ubam upserts" in {
    val key = TransferWgsUbamV1Key(
      "testClioIdBarcode",
      2,
      s"library$randomId",
      Location.GCP
    )

    for {
      clioId1 <- runUpsert(
        key,
        TransferWgsUbamV1Metadata(project = Some("testProject1"))
      )
      clioId2 <- runUpsert(
        key,
        TransferWgsUbamV1Metadata(project = Some("testProject2"))
      )
    } yield {
      clioId2.compareTo(clioId1) should be(1)

      val storedDocument1 =
        getJsonFrom[DocumentWgsUbam](ElasticsearchIndex.WgsUbam, clioId1)
      storedDocument1.project should be(Some("testProject1"))

      val storedDocument2 =
        getJsonFrom[DocumentWgsUbam](ElasticsearchIndex.WgsUbam, clioId2)
      storedDocument2.project should be(Some("testProject2"))

      storedDocument1.copy(clioId = clioId2, project = Some("testProject2")) should be(
        storedDocument2
      )
    }
  }

  it should "assign different clioIds to equal wgs-ubam upserts" in {
    val key = TransferWgsUbamV1Key(
      "testClioIdBarcode",
      2,
      s"library$randomId",
      Location.GCP
    )
    val metadata = TransferWgsUbamV1Metadata(project = Some("testProject1"))

    for {
      clioId1 <- runUpsert(key, metadata)
      clioId2 <- runUpsert(key, metadata)
    } yield {
      clioId2.compareTo(clioId1) should be(1)

      val storedDocument1 =
        getJsonFrom[DocumentWgsUbam](ElasticsearchIndex.WgsUbam, clioId1)
      val storedDocument2 =
        getJsonFrom[DocumentWgsUbam](ElasticsearchIndex.WgsUbam, clioId2)
      storedDocument1.copy(clioId = clioId2) should be(storedDocument2)
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
          val metadata = TransferWgsUbamV1Metadata(
            project = Some(project),
            sampleAlias = Some(sample)
          )
          runUpsert(key, metadata)
      }
    }

    for {
      _ <- upserts
      projectResponse <- runClient(WgsUbamTests.query, "--project", project)
      projectResults <- Unmarshal(projectResponse)
        .to[Seq[TransferWgsUbamV1QueryOutput]]
      sampleResponse <- runClient(
        WgsUbamTests.query,
        "--sample-alias",
        samples.head
      )
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
    val key =
      TransferWgsUbamV1Key("barcode2", 2, s"library$randomId", Location.GCP)
    val project = s"testProject$randomId"
    val metadata = TransferWgsUbamV1Metadata(
      project = Some(project),
      sampleAlias = Some("sampleAlias1"),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        response <- runClient(WgsUbamTests.query, "--project", project)
        results <- Unmarshal(response).to[Seq[TransferWgsUbamV1QueryOutput]]
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = TransferWgsUbamV1Metadata(
      sampleAlias = metadata.sampleAlias,
      project = metadata.project
    )

    for {
      _ <- runUpsert(key, upsertData)
      original <- query
      _ = original.sampleAlias should be(metadata.sampleAlias)
      _ = original.notes should be(None)
      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsert(key, upsertData2)
      withNotes <- query
      _ = withNotes.sampleAlias should be(metadata.sampleAlias)
      _ = withNotes.notes should be(metadata.notes)
      _ <- runUpsert(
        key,
        upsertData2.copy(sampleAlias = Some("sampleAlias2"), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.sampleAlias should be(Some("sampleAlias2"))
      emptyNotes.notes should be(Some(""))
    }
  }

  it should "show deleted records on queryall, but not query" in {
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
        case (key, metadata) => runUpsert(key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        response <- runClient(
          WgsUbamTests.query,
          "--project",
          project,
          "--flowcell-barcode",
          barcode
        )
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
      _ <- upserts
      _ <- checkQuery(expectedLength = 3)
      // TODO: This should use the delete CLP once we figure out how to test the I/O portion.
      deleteResponse <- runUpsert(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ <- checkQuery(expectedLength = 2)

      response <- runClient(
        WgsUbamTests.query,
        "--project",
        project,
        "--flowcell-barcode",
        barcode,
        "--include-deleted"
      )
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

/** Command-line constants for wgs-ubam CLPs. */
object WgsUbamTests {

  /** The CLP command for querying wgs-ubams. */
  val query = "query-wgs-ubam"

  /** The CLP command for upsert-ing wgs-ubams. */
  val upsert = "add-wgs-ubam"
}
