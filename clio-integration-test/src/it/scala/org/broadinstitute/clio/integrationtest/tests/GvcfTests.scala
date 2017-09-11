package org.broadinstitute.clio.integrationtest.tests

import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentGvcf,
  ElasticsearchIndex
}
import org.broadinstitute.clio.transfer.model.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryOutput
}
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import akka.http.scaladsl.unmarshalling.Unmarshal
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json

import scala.concurrent.Future

import java.nio.file.Files
import java.util.UUID

/** Tests of Clio's gvcf functionality. */
trait GvcfTests { self: BaseIntegrationSpec =>

  def runUpsertGvcf(key: TransferGvcfV1Key,
                    metadata: TransferGvcfV1Metadata): Future[UUID] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runClient(
      ClioCommand.addGvcfName,
      "--location",
      key.location.entryName,
      "--project",
      key.project,
      "--sample-alias",
      key.sampleAlias,
      "--version",
      key.version.toString,
      "--metadata-location",
      tmpMetadata.toString
    ).flatMap(Unmarshal(_).to[UUID])
  }

  it should "create the expected gvcf mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._

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
    runClient(ClioCommand.getGvcfSchemaName)
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

    /*
     * NOTE: This is lazy on purpose. If it executes outside of the actual `it` block,
     * it'll result in an `UninitializedFieldError` because the spec `beforeAll` won't
     * have triggered yet.
     */
    lazy val responseFuture = runUpsertGvcf(
      TransferGvcfV1Key(
        location,
        expected.project,
        expected.sampleAlias,
        expected.version
      ),
      TransferGvcfV1Metadata(gvcfPath = Some("gs://path/gvcf.gvcf"))
    )

    if (location == Location.Unknown) {
      it should "reject gvcf inputs with unknown location" in {
        recoverToSucceededIf[Exception](responseFuture)
      }
    } else {
      it should s"handle upserts and queries for gvcf location $location" in {
        for {
          returnedClioId <- responseFuture
          queryResponse <- runClient(
            ClioCommand.queryGvcfName,
            "--location",
            location.entryName
          )
          outputs <- Unmarshal(queryResponse)
            .to[Seq[TransferGvcfV1QueryOutput]]
        } yield {
          outputs should have length 1
          outputs.head should be(expected)

          val storedDocument =
            getJsonFrom[DocumentGvcf](ElasticsearchIndex.Gvcf, returnedClioId)
          storedDocument.location should be(expected.location)
          storedDocument.project should be(expected.project)
          storedDocument.sampleAlias should be(expected.sampleAlias)
          storedDocument.version should be(expected.version)
          storedDocument.gvcfPath should be(expected.gvcfPath)
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

    for {
      clioId1 <- runUpsertGvcf(
        upsertKey,
        TransferGvcfV1Metadata(gvcfPath = Some("gs://path/gvcf1.gvcf"))
      )
      clioId2 <- clioWebClient
        .addGvcf(
          upsertKey,
          TransferGvcfV1Metadata(gvcfPath = Some("gs://path/gvcf2.gvcf"))
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
      clioId1 <- runUpsertGvcf(upsertKey, upsertData)
      clioId2 <- runUpsertGvcf(upsertKey, upsertData)
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
          runUpsertGvcf(key, data)
      }
    }

    for {
      _ <- upserts
      projectResponse <- runClient(
        ClioCommand.queryGvcfName,
        "--project",
        project
      )
      projectResults <- Unmarshal(projectResponse)
        .to[Seq[TransferGvcfV1QueryOutput]]
      sampleResponse <- runClient(
        ClioCommand.queryGvcfName,
        "--sample-alias",
        samples.head
      )
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
    val project = s"testProject$randomId"
    val key =
      TransferGvcfV1Key(Location.GCP, project, s"testSample$randomId", 1)
    val gvcfPath = "gs://path/gvcf.gvcf"
    val metadata = TransferGvcfV1Metadata(
      gvcfPath = Some(gvcfPath),
      contamination = Some(.75f),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        response <- runClient(ClioCommand.queryGvcfName, "--project", project)
        results <- Unmarshal(response).to[Seq[TransferGvcfV1QueryOutput]]
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = TransferGvcfV1Metadata(
      contamination = metadata.contamination,
      gvcfPath = metadata.gvcfPath
    )

    for {
      _ <- runUpsertGvcf(key, upsertData)
      original <- query
      _ = original.gvcfPath should be(metadata.gvcfPath)
      _ = original.contamination should be(metadata.contamination)
      _ = original.notes should be(None)
      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertGvcf(key, upsertData2)
      withNotes <- query
      _ = original.gvcfPath should be(metadata.gvcfPath)
      _ = withNotes.contamination should be(metadata.contamination)
      _ = withNotes.notes should be(metadata.notes)
      _ <- runUpsertGvcf(
        key,
        upsertData2
          .copy(contamination = Some(0.123f), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.gvcfPath should be(metadata.gvcfPath)
      emptyNotes.contamination should be(Some(0.123f))
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
        case (key, metadata) => runUpsertGvcf(key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        response <- runClient(
          ClioCommand.queryGvcfName,
          "--project",
          project,
          "--sample-alias",
          sampleAlias
        )
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
      _ <- upserts
      _ <- checkQuery(expectedLength = 3)
      deleteResponse <- runUpsertGvcf(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ <- checkQuery(expectedLength = 2)

      response <- runClient(
        ClioCommand.queryGvcfName,
        "--project",
        project,
        "--sample-alias",
        sampleAlias,
        "--include-deleted"
      )
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

  it should "move gvcfs in GCP" in {
    val project = s"abcdefg$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val fileContents = s"$randomId --- I am a dummy gvcf --- $randomId"
    val cloudPath = rootTestStorageDir.resolve(
      s"gvcf/$project/$sample/v$version/$randomId.vcf.gz"
    )
    val cloudPath2 = cloudPath.getParent.resolve(s"moved/$randomId.vcf/gz")

    val key = TransferGvcfV1Key(Location.GCP, project, sample, version)
    val metadata =
      TransferGvcfV1Metadata(gvcfPath = Some(cloudPath.toUri.toString))

    // Clio needs the metadata to be added before it can be moved.
    val _ = Files.write(cloudPath, fileContents.getBytes)
    val result = for {
      _ <- runUpsertGvcf(key, metadata)
      _ <- runClient(
        ClioCommand.moveGvcfName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--sample-alias",
        sample,
        "--version",
        version.toString,
        "--destination",
        cloudPath2.toUri.toString
      )
    } yield {
      Files.exists(cloudPath) should be(false)
      Files.exists(cloudPath2) should be(true)
      new String(Files.readAllBytes(cloudPath2)) should be(fileContents)
    }

    result.andThen[Unit] {
      case _ => {
        val _ = Seq(cloudPath, cloudPath2).map(Files.deleteIfExists)
      }
    }
  }

  it should "delete gvcfs in GCP" in {
    val project = s"abcdefg$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val fileContents = s"$randomId --- I am fated to die --- $randomId"
    val cloudPath = rootTestStorageDir.resolve(
      s"gvcf/$project/$sample/v$version/$randomId.vcf.gz"
    )
    val deleteNote =
      s"""Deleted by the integration tests, contents were:
         |$fileContents
       """.stripMargin

    val key = TransferGvcfV1Key(Location.GCP, project, sample, version)
    val metadata =
      TransferGvcfV1Metadata(gvcfPath = Some(cloudPath.toUri.toString))

    // Clio needs the metadata to be added before it can be deleted.
    val _ = Files.write(cloudPath, fileContents.getBytes)
    val result = for {
      _ <- runUpsertGvcf(key, metadata)
      _ <- runClient(
        ClioCommand.deleteGvcfName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--sample-alias",
        sample,
        "--version",
        version.toString,
        "--note",
        deleteNote
      )
      _ = Files.exists(cloudPath) should be(false)
      response <- runClient(
        ClioCommand.queryGvcfName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--sample-alias",
        sample,
        "--version",
        version.toString,
        "--include-deleted"
      )
      outputs <- Unmarshal(response).to[Seq[TransferGvcfV1QueryOutput]]
    } yield {
      outputs should have length 1
      outputs.head.notes should be(Some(deleteNote))
      outputs.head.documentStatus should be(Some(DocumentStatus.Deleted))
    }

    result.andThen[Unit] {
      case _ => {
        val _ = Files.deleteIfExists(cloudPath)
      }
    }
  }
}
