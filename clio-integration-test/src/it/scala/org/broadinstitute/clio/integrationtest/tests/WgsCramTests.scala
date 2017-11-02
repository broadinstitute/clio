package org.broadinstitute.clio.integrationtest.tests

import java.net.URI
import java.nio.file.Files

import akka.http.scaladsl.unmarshalling.Unmarshal
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentWgsCram,
  ElasticsearchIndex
}
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram._
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation,
  UpsertId
}

import scala.concurrent.Future

/** Tests of Clio's wgs-cram functionality. */
trait WgsCramTests { self: BaseIntegrationSpec =>

  def runUpsertCram(key: TransferWgsCramV1Key,
                    metadata: TransferWgsCramV1Metadata): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runClient(
      ClioCommand.addWgsCramName,
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
    ).flatMap(Unmarshal(_).to[UpsertId])
  }

  it should "create the expected wgs-cram mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val expected = ElasticsearchIndex.WgsCram
    val getRequest =
      getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.execute(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
  }

  it should "report the expected JSON schema for wgs-cram" in {
    runClient(ClioCommand.getWgsCramSchemaName)
      .flatMap(Unmarshal(_).to[Json])
      .map(_ should be(WgsCramIndex.jsonSchema))
  }

  // Generate a test for every possible Location value.
  Location.values.foreach {
    it should behave like testCramLocation(_)
  }

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testCramLocation(location: Location): Unit = {
    val expected = TransferWgsCramV1QueryOutput(
      location = location,
      project = "testProject",
      sampleAlias = s"someAlias$randomId",
      version = 2,
      documentStatus = Some(DocumentStatus.Normal),
      regulatoryDesignation = Some(RegulatoryDesignation.ResearchOnly),
      cramPath = Some(URI.create("gs://path/cram.cram"))
    )

    /*
     * NOTE: This is lazy on purpose. If it executes outside of the actual `it` block,
     * it'll result in an `UninitializedFieldError` because the spec `beforeAll` won't
     * have triggered yet.
     */
    lazy val responseFuture = runUpsertCram(
      TransferWgsCramV1Key(
        location,
        expected.project,
        expected.sampleAlias,
        expected.version
      ),
      TransferWgsCramV1Metadata(cramPath = expected.cramPath)
    )

    if (location == Location.Unknown) {
      it should "reject wgs-cram inputs with unknown location" in {
        recoverToSucceededIf[Exception](responseFuture)
      }
    } else {
      it should s"handle upserts and queries for wgs-cram location $location" in {
        for {
          returnedUpsertId <- responseFuture
          queryResponse <- runClient(
            ClioCommand.queryWgsCramName,
            "--sample-alias",
            expected.sampleAlias
          )
          outputs <- Unmarshal(queryResponse)
            .to[Seq[TransferWgsCramV1QueryOutput]]
        } yield {
          outputs should have length 1
          outputs.head should be(expected)

          val storedDocument =
            getJsonFrom[DocumentWgsCram](
              ElasticsearchIndex.WgsCram,
              returnedUpsertId
            )
          storedDocument.location should be(expected.location)
          storedDocument.project should be(expected.project)
          storedDocument.sampleAlias should be(expected.sampleAlias)
          storedDocument.version should be(expected.version)
          storedDocument.cramPath should be(expected.cramPath)
        }
      }
    }
  }

  it should "assign different upsertIds to different wgs-cram upserts" in {
    val upsertKey = TransferWgsCramV1Key(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )

    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        TransferWgsCramV1Metadata(
          cramPath = Some(URI.create("gs://path/cram1.cram"))
        )
      )
      upsertId2 <- runUpsertCram(
        upsertKey,
        TransferWgsCramV1Metadata(
          cramPath = Some(URI.create("gs://path/cram2.cram"))
        )
      )
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 =
        getJsonFrom[DocumentWgsCram](ElasticsearchIndex.WgsCram, upsertId1)
      storedDocument1.cramPath should be(
        Some(URI.create("gs://path/cram1.cram"))
      )

      val storedDocument2 =
        getJsonFrom[DocumentWgsCram](ElasticsearchIndex.WgsCram, upsertId2)
      storedDocument2.cramPath should be(
        Some(URI.create("gs://path/cram2.cram"))
      )

      storedDocument1.copy(
        upsertId = upsertId2,
        cramPath = Some(URI.create("gs://path/cram2.cram"))
      ) should be(storedDocument2)
    }
  }

  it should "assign different upsertIds to equal wgs-cram upserts" in {
    val upsertKey = TransferWgsCramV1Key(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )
    val upsertData =
      TransferWgsCramV1Metadata(
        cramPath = Some(URI.create("gs://path/cram1.cram"))
      )

    for {
      upsertId1 <- runUpsertCram(upsertKey, upsertData)
      upsertId2 <- runUpsertCram(upsertKey, upsertData)
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 =
        getJsonFrom[DocumentWgsCram](ElasticsearchIndex.WgsCram, upsertId1)
      val storedDocument2 =
        getJsonFrom[DocumentWgsCram](ElasticsearchIndex.WgsCram, upsertId2)
      storedDocument1.copy(upsertId = upsertId2) should be(storedDocument2)
    }
  }

  it should "handle querying wgs-crams by sample and project" in {
    val location = Location.GCP
    val project = "testProject" + randomId

    val samples = {
      val sameId = "testSample" + randomId
      Seq(sameId, sameId, "testSample" + randomId)
    }

    val upserts = Future.sequence {
      samples.zip(1 to 3).map {
        case (sample, version) =>
          val key = TransferWgsCramV1Key(location, project, sample, version)
          val data = TransferWgsCramV1Metadata(
            cramPath = Some(URI.create("gs://path/cram.cram")),
            cramSize = Some(1000L)
          )
          runUpsertCram(key, data)
      }
    }

    for {
      _ <- upserts
      projectResponse <- runClient(
        ClioCommand.queryWgsCramName,
        "--project",
        project
      )
      projectResults <- Unmarshal(projectResponse)
        .to[Seq[TransferWgsCramV1QueryOutput]]
      sampleResponse <- runClient(
        ClioCommand.queryWgsCramName,
        "--sample-alias",
        samples.head
      )
      sampleResults <- Unmarshal(sampleResponse)
        .to[Seq[TransferWgsCramV1QueryOutput]]
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

  it should "handle updates to wgs-cram metadata" in {
    val project = s"testProject$randomId"
    val key =
      TransferWgsCramV1Key(Location.GCP, project, s"testSample$randomId", 1)
    val cramPath = URI.create("gs://path/cram.cram")
    val metadata = TransferWgsCramV1Metadata(
      cramPath = Some(cramPath),
      cramSize = Some(1000L),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        response <- runClient(
          ClioCommand.queryWgsCramName,
          "--project",
          project
        )
        results <- Unmarshal(response).to[Seq[TransferWgsCramV1QueryOutput]]
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = TransferWgsCramV1Metadata(
      cramSize = metadata.cramSize,
      cramPath = metadata.cramPath
    )

    for {
      _ <- runUpsertCram(key, upsertData)
      original <- query
      _ = original.cramPath should be(metadata.cramPath)
      _ = original.cramSize should be(metadata.cramSize)
      _ = original.notes should be(None)
      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertCram(key, upsertData2)
      withNotes <- query
      _ = original.cramPath should be(metadata.cramPath)
      _ = withNotes.cramSize should be(metadata.cramSize)
      _ = withNotes.notes should be(metadata.notes)
      _ <- runUpsertCram(
        key,
        upsertData2
          .copy(cramSize = Some(2000L), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.cramPath should be(metadata.cramPath)
      emptyNotes.cramSize should be(Some(2000L))
      emptyNotes.notes should be(Some(""))
    }
  }

  it should "show deleted wgs-cram records on queryAll, but not query" in {
    val project = "testProject" + randomId
    val sampleAlias = "sample688." + randomId

    val keysWithMetadata = (1 to 3).map { version =>
      val upsertKey = TransferWgsCramV1Key(
        location = Location.GCP,
        project = project,
        sampleAlias = sampleAlias,
        version = version
      )
      val upsertMetadata = TransferWgsCramV1Metadata(
        cramPath = Some(URI.create(s"gs://cram/$sampleAlias.$version"))
      )
      (upsertKey, upsertMetadata)
    }
    val (deleteKey, deleteData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (key, metadata) => runUpsertCram(key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        response <- runClient(
          ClioCommand.queryWgsCramName,
          "--project",
          project,
          "--sample-alias",
          sampleAlias
        )
        results <- Unmarshal(response).to[Seq[TransferWgsCramV1QueryOutput]]
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
      _ <- runUpsertCram(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ <- checkQuery(expectedLength = 2)

      response <- runClient(
        ClioCommand.queryWgsCramName,
        "--project",
        project,
        "--sample-alias",
        sampleAlias,
        "--include-deleted"
      )
      results <- Unmarshal(response).to[Seq[TransferWgsCramV1QueryOutput]]
    } yield {
      results.length should be(keysWithMetadata.length)
      results.foldLeft(succeed) { (_, result) =>
        result.project should be(project)
        result.sampleAlias should be(sampleAlias)

        val resultKey = TransferWgsCramV1Key(
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

  it should "move wgs-crams in GCP" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val fileContents = s"$randomId --- I am a dummy cram --- $randomId"
    val cloudPath = rootTestStorageDir.resolve(
      s"cram/$project/$sample/v$version/$randomId.cram"
    )
    val cloudPath2 = cloudPath.getParent.resolve(s"moved/$randomId.cram")

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata =
      TransferWgsCramV1Metadata(cramPath = Some(cloudPath.toUri))

    // Clio needs the metadata to be added before it can be moved.
    val _ = Files.write(cloudPath, fileContents.getBytes)
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runClient(
        ClioCommand.moveWgsCramName,
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
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Seq(cloudPath, cloudPath2).map(Files.deleteIfExists)
      }
    }
  }

  it should "move the cram and crai together in GCP" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cramContents = s"$randomId --- I am a dummy cram --- $randomId"
    val craiContents = s"$randomId --- I am a dummy crai --- $randomId"
    val alignmentMetricsContents =
      s"$randomId --- I am dummy alignment metrics --- $randomId"
    val fingerprintMetricsContents =
      s"$randomId --- I am dummy fingerprinting metrics --- $randomId"

    val cramName = s"$randomId.cram"
    val craiName = s"$randomId.crai"
    val alignmentMetricsName = s"$randomId.metrics"
    val fingerprintMetricsName = s"$randomId.metrics"

    val rootSource =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramSource = rootSource.resolve(cramName)
    val craiSource = rootSource.resolve(craiName)
    val alignmentMetricsSource = rootSource.resolve(alignmentMetricsName)
    val fingerprintMetricsSource = rootSource.resolve(fingerprintMetricsName)

    val rootDestination = rootSource.getParent.resolve(s"moved/$randomId/")
    val cramDestination = rootDestination.resolve(cramName)
    val craiDestination = rootDestination.resolve(craiName)
    val alignmentMetricsDestination =
      rootDestination.resolve(alignmentMetricsName)
    val fingerprintMetricsDestination =
      rootDestination.resolve(fingerprintMetricsName)

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata = TransferWgsCramV1Metadata(
      cramPath = Some(cramSource.toUri),
      craiPath = Some(craiSource.toUri),
      alignmentSummaryMetricsPath = Some(alignmentMetricsSource.toUri),
      fingerprintingSummaryMetricsPath = Some(fingerprintMetricsSource.toUri)
    )

    val _ = Seq(
      (cramSource, cramContents),
      (craiSource, craiContents),
      (alignmentMetricsSource, alignmentMetricsContents),
      (fingerprintMetricsSource, fingerprintMetricsContents)
    ).map {
      case (source, contents) => Files.write(source, contents.getBytes)
    }
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runClient(
        ClioCommand.moveWgsCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--sample-alias",
        sample,
        "--version",
        version.toString,
        "--destination",
        rootDestination.toUri.toString
      )
    } yield {
      Seq(cramSource, craiSource).foreach(Files.exists(_) should be(false))
      Files.exists(alignmentMetricsSource) should be(true)

      Seq(cramDestination, craiDestination)
        .foreach(Files.exists(_) should be(true))

      Files.exists(alignmentMetricsDestination) should be(false)

      // We don't deliver fingerprinting metrics for now because they're based on unpublished research.
      Files.exists(fingerprintMetricsSource) should be(true)
      Files.exists(fingerprintMetricsDestination) should be(false)

      Seq(
        (cramDestination, cramContents),
        (craiDestination, craiContents),
        (alignmentMetricsSource, alignmentMetricsContents),
        (fingerprintMetricsSource, fingerprintMetricsContents)
      ).foreach {
        case (destination, contents) =>
          new String(Files.readAllBytes(destination)) should be(contents)
      }
      succeed
    }

    result.andThen {
      case _ => {
        val _ = Seq(
          cramSource,
          cramDestination,
          craiSource,
          craiDestination,
          alignmentMetricsSource,
          alignmentMetricsDestination,
          fingerprintMetricsSource,
          fingerprintMetricsDestination
        ).map(Files.deleteIfExists)
      }
    }
  }

  it should "not move wgs-crams without a destination" in {
    recoverToExceptionIf[Exception] {
      runClient(
        ClioCommand.moveWgsCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        randomId,
        "--sample-alias",
        randomId,
        "--version",
        "123"
      )
    }.map {
      _.getMessage should include("--destination")
    }
  }

  def addAndDeleteCram(
    deleteNote: String,
    existingNote: Option[String]
  ): Future[TransferWgsCramV1QueryOutput] = {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val fileContents = s"$randomId --- I am fated to die --- $randomId"
    val cloudPath = rootTestStorageDir.resolve(
      s"cram/$project/$sample/v$version/$randomId.cram"
    )

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata =
      TransferWgsCramV1Metadata(
        cramPath = Some(cloudPath.toUri),
        notes = existingNote
      )

    // Clio needs the metadata to be added before it can be deleted.
    val _ = Files.write(cloudPath, fileContents.getBytes)
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runClient(
        ClioCommand.deleteWgsCramName,
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
        ClioCommand.queryWgsCramName,
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
      outputs <- Unmarshal(response).to[Seq[TransferWgsCramV1QueryOutput]]
    } yield {
      outputs should have length 1
      outputs.head
    }

    result.andThen[Unit] {
      case _ => {
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Files.deleteIfExists(cloudPath)
      }
    }
  }

  it should "delete wgs-crams in GCP" in {
    val deleteNote =
      s"$randomId --- Deleted by the integration tests --- $randomId"

    val deletedWithNoExistingNote = addAndDeleteCram(deleteNote, None)
    deletedWithNoExistingNote.map { output =>
      output.notes should be(Some(deleteNote))
      output.documentStatus should be(Some(DocumentStatus.Deleted))
    }
  }

  it should "preserve existing notes when deleting wgs-crams" in {
    val deleteNote =
      s"$randomId --- Deleted by the integration tests --- $randomId"
    val existingNote = s"$randomId --- I am an existing note --- $randomId"

    val deletedWithExistingNote =
      addAndDeleteCram(deleteNote, Some(existingNote))
    deletedWithExistingNote.map { output =>
      output.notes should be(Some(s"$existingNote\n$deleteNote"))
      output.documentStatus should be(Some(DocumentStatus.Deleted))
    }
  }

  it should "delete the cram and crai, but not metrics" in {
    val deleteNote =
      s"$randomId --- Deleted by the integration tests --- $randomId"

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cramContents = s"$randomId --- I am a cram fated to die --- $randomId"
    val craiContents =
      s"$randomId --- I am an index fated to die --- $randomId"
    val metrics1Contents =
      s"$randomId --- I am an immortal metrics file --- $randomId"
    val metrics2Contents =
      s"$randomId --- I am a second immortal metrics file --- $randomId"

    val storageDir =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramPath = storageDir.resolve(s"$randomId.cram")
    val craiPath = storageDir.resolve(s"$randomId.crai")
    val metrics1Path = storageDir.resolve(s"$randomId.metrics")
    val metrics2Path = storageDir.resolve(s"$randomId.metrics")

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata =
      TransferWgsCramV1Metadata(
        cramPath = Some(cramPath.toUri),
        craiPath = Some(craiPath.toUri),
        alignmentSummaryMetricsPath = Some(metrics1Path.toUri),
        fingerprintingSummaryMetricsPath = Some(metrics1Path.toUri)
      )

    val _ = Seq(
      (cramPath, cramContents),
      (craiPath, craiContents),
      (metrics1Path, metrics1Contents),
      (metrics2Path, metrics2Contents)
    ).map {
      case (path, contents) => Files.write(path, contents.getBytes)
    }
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runClient(
        ClioCommand.deleteWgsCramName,
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
      response <- runClient(
        ClioCommand.queryWgsCramName,
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
      outputs <- Unmarshal(response).to[Seq[TransferWgsCramV1QueryOutput]]
    } yield {
      Seq(cramPath, craiPath).foreach {
        Files.exists(_) should be(false)
      }
      Seq((metrics1Path, metrics1Contents), (metrics2Path, metrics2Contents))
        .foreach {
          case (path, contents) => {
            Files.exists(path) should be(true)
            new String(Files.readAllBytes(path)) should be(contents)
          }
        }

      outputs should have length 1
      outputs.head.notes should be(Some(deleteNote))
      outputs.head.documentStatus should be(Some(DocumentStatus.Deleted))
    }

    result.andThen[Unit] {
      case _ => {
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Seq(cramPath, craiPath, metrics1Path, metrics2Path)
          .map(Files.deleteIfExists)
      }
    }
  }

  it should "not delete wgs-crams without a note" in {
    recoverToExceptionIf[Exception] {
      runClient(
        ClioCommand.deleteWgsCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        randomId,
        "--sample-alias",
        randomId,
        "--version",
        "123"
      )
    }.map {
      _.getMessage should include("--note")
    }
  }

  it should "move files, generate an md5 file, and record the workspace name when delivering crams" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cramContents = s"$randomId --- I am a dummy cram --- $randomId"
    val craiContents = s"$randomId --- I am a dummy crai --- $randomId"
    val md5Contents = randomId

    val cramName = s"$randomId.cram"
    val craiName = s"$cramName.crai"
    val md5Name = s"$cramName.md5"

    val rootSource =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramSource = rootSource.resolve(cramName)
    val craiSource = rootSource.resolve(craiName)

    val rootDestination = rootSource.getParent.resolve(s"moved/$randomId/")
    val cramDestination = rootDestination.resolve(cramName)
    val craiDestination = rootDestination.resolve(craiName)
    val md5Destination = rootDestination.resolve(md5Name)

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata = TransferWgsCramV1Metadata(
      cramPath = Some(cramSource.toUri),
      craiPath = Some(craiSource.toUri),
      cramMd5 = Some(Symbol(md5Contents))
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    val _ = Seq((cramSource, cramContents), (craiSource, craiContents)).map {
      case (source, contents) => Files.write(source, contents.getBytes)
    }
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runClient(
        ClioCommand.deliverWgsCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--sample-alias",
        sample,
        "--version",
        version.toString,
        "--workspace-name",
        workspaceName,
        "--workspace-path",
        rootDestination.toUri.toString
      )
      response <- runClient(
        ClioCommand.queryWgsCramName,
        "--workspace-name",
        workspaceName
      )
      outputs <- Unmarshal(response).to[Seq[TransferWgsCramV1QueryOutput]]
    } yield {
      Seq(cramSource, craiSource).foreach(Files.exists(_) should be(false))

      Seq(cramDestination, craiDestination, md5Destination).foreach(
        Files.exists(_) should be(true)
      )

      Seq(
        (cramDestination, cramContents),
        (craiDestination, craiContents),
        (md5Destination, md5Contents)
      ).foreach {
        case (destination, contents) =>
          new String(Files.readAllBytes(destination)) should be(contents)
      }

      outputs should be {
        Seq(
          TransferWgsCramV1QueryOutput(
            location = Location.GCP,
            project = project,
            sampleAlias = sample,
            version = version,
            workspaceName = Some(workspaceName),
            cramPath = Some(cramDestination.toUri),
            craiPath = Some(craiDestination.toUri),
            cramMd5 = Some(Symbol(md5Contents)),
            documentStatus = Some(DocumentStatus.Normal),
            regulatoryDesignation = Some(RegulatoryDesignation.ResearchOnly)
          )
        )
      }
    }

    result.andThen {
      case _ => {
        val _ = Seq(
          cramSource,
          cramDestination,
          craiSource,
          craiDestination,
          md5Destination
        ).map(Files.deleteIfExists)
      }
    }
  }

  it should "automatically set regulatory designation to ResearchOnly for crams" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val md5Contents = randomId

    val cramName = s"$randomId.cram"
    val craiName = s"$cramName.crai"

    val rootSource =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramSource = rootSource.resolve(cramName)
    val craiSource = rootSource.resolve(craiName)

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata = TransferWgsCramV1Metadata(
      cramPath = Some(cramSource.toUri),
      craiPath = Some(craiSource.toUri),
      cramMd5 = Some(Symbol(md5Contents)),
      regulatoryDesignation = None
    )

    def query = {
      for {
        response <- runClient(
          ClioCommand.queryWgsCramName,
          "--project",
          project
        )
        results <- Unmarshal(response).to[Seq[TransferWgsCramV1QueryOutput]]
      } yield {
        results should have length 1
        results.head
      }
    }

    for {
      _ <- runUpsertCram(key, metadata)
      result <- query
    } yield {
      result.regulatoryDesignation should be(
        Some(RegulatoryDesignation.ResearchOnly)
      )
    }

  }

  it should "fail delivery if the underlying move fails" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val md5Contents = randomId

    val cramName = s"$randomId.cram"
    val craiName = s"$cramName.crai"

    val rootSource =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramSource = rootSource.resolve(cramName)
    val craiSource = rootSource.resolve(craiName)

    val rootDestination = rootSource.getParent.resolve(s"moved/$randomId/")

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata = TransferWgsCramV1Metadata(
      cramPath = Some(cramSource.toUri),
      craiPath = Some(craiSource.toUri),
      cramMd5 = Some(Symbol(md5Contents))
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    recoverToExceptionIf[Exception] {
      for {
        _ <- runUpsertCram(key, metadata)
        // Should fail because the source files don't exist.
        deliverResponse <- runClient(
          ClioCommand.deliverWgsCramName,
          "--location",
          Location.GCP.entryName,
          "--project",
          project,
          "--sample-alias",
          sample,
          "--version",
          version.toString,
          "--workspace-name",
          workspaceName,
          "--workspace-path",
          rootDestination.toUri.toString
        )
      } yield {
        deliverResponse
      }
    }.flatMap { _ =>
      for {
        response <- runClient(
          ClioCommand.queryWgsCramName,
          "--workspace-name",
          workspaceName
        )
        outputs <- Unmarshal(response).to[Seq[TransferWgsCramV1QueryOutput]]
      } yield {
        // The CLP shouldn't have tried to upsert the workspace name.
        outputs shouldBe empty
      }
    }
  }
}
