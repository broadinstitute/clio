package org.broadinstitute.clio.integrationtest.tests

import java.net.URI
import java.nio.file.Files

import com.sksamuel.elastic4s.IndexAndType
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentWgsCram,
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram._
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation,
  UpsertId
}
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests of Clio's wgs-cram functionality. */
trait WgsCramTests { self: BaseIntegrationSpec =>

  def runUpsertCram(
    key: TransferWgsCramV1Key,
    metadata: TransferWgsCramV1Metadata
  ): Future[UpsertId] = {
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
    ).mapTo[UpsertId]
  }

  it should "create the expected wgs-cram mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

    val expected = ElasticsearchIndex[DocumentWgsCram]
    val getRequest = getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
  }

  it should "report the expected JSON schema for wgs-cram" in {
    runClient(ClioCommand.getWgsCramSchemaName)
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
      project = "test project",
      sampleAlias = s"someAlias $randomId",
      version = 2,
      documentStatus = Some(DocumentStatus.Normal),
      cramPath = Some(URI.create(s"gs://path/cram${WgsCramExtensions.CramExtension}"))
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

    it should s"handle upserts and queries for wgs-cram location $location" in {
      for {
        returnedUpsertId <- responseFuture
        outputs <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
          ClioCommand.queryWgsCramName,
          "--sample-alias",
          expected.sampleAlias
        )
      } yield {
        outputs should be(Seq(expected))

        val storedDocument = getJsonFrom[DocumentWgsCram](returnedUpsertId)
        storedDocument.location should be(expected.location)
        storedDocument.project should be(expected.project)
        storedDocument.sampleAlias should be(expected.sampleAlias)
        storedDocument.version should be(expected.version)
        storedDocument.cramPath should be(expected.cramPath)
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
          cramPath = Some(
            URI.create(s"gs://path/cram1${WgsCramExtensions.CramExtension}")
          )
        )
      )
      upsertId2 <- runUpsertCram(
        upsertKey,
        TransferWgsCramV1Metadata(
          cramPath = Some(
            URI.create(s"gs://path/cram2${WgsCramExtensions.CramExtension}")
          )
        )
      )
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 =
        getJsonFrom[DocumentWgsCram](upsertId1)
      storedDocument1.cramPath should be(
        Some(URI.create(s"gs://path/cram1${WgsCramExtensions.CramExtension}"))
      )

      val storedDocument2 =
        getJsonFrom[DocumentWgsCram](upsertId2)
      storedDocument2.cramPath should be(
        Some(URI.create(s"gs://path/cram2${WgsCramExtensions.CramExtension}"))
      )

      storedDocument1.copy(
        upsertId = upsertId2,
        cramPath = Some(URI.create(s"gs://path/cram2${WgsCramExtensions.CramExtension}"))
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
        cramPath = Some(URI.create(s"gs://path/cram1${WgsCramExtensions.CramExtension}"))
      )

    for {
      upsertId1 <- runUpsertCram(upsertKey, upsertData)
      upsertId2 <- runUpsertCram(upsertKey, upsertData)
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 = getJsonFrom[DocumentWgsCram](upsertId1)
      val storedDocument2 = getJsonFrom[DocumentWgsCram](upsertId2)
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
            cramPath = Some(
              URI.create(s"gs://path/cram${WgsCramExtensions.CramExtension}")
            ),
            cramSize = Some(1000L)
          )
          runUpsertCram(key, data)
      }
    }

    for {
      _ <- upserts
      projectResults <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--project",
        project
      )
      sampleResults <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--sample-alias",
        samples.head
      )
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

  it should "only return exact matches for string queries" in {
    val location = Location.GCP
    val project = s"testProject$randomId"

    val prefix = s"testSample$randomId"
    val suffix = s"${randomId}testSample"
    val samples = Seq(prefix, suffix, s"$prefix-$suffix")

    val upserts = Future.sequence {
      samples.zip(1 to 3).map {
        case (sample, version) =>
          val key = TransferWgsCramV1Key(location, project, sample, version)
          val data = TransferWgsCramV1Metadata(
            cramPath = Some(
              URI.create(s"gs://path/cram${WgsCramExtensions.CramExtension}")
            ),
            cramSize = Some(1000L)
          )
          runUpsertCram(key, data)
      }
    }

    for {
      _ <- upserts
      prefixResults <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--sample-alias",
        prefix
      )
      suffixResults <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--sample-alias",
        suffix
      )
    } yield {
      prefixResults should have length 1
      suffixResults should have length 1
    }
  }

  it should "handle updates to wgs-cram metadata" in {
    val project = s"testProject$randomId"
    val key =
      TransferWgsCramV1Key(Location.GCP, project, s"testSample$randomId", 1)
    val cramPath =
      URI.create(s"gs://path/cram${WgsCramExtensions.CramExtension}")
    val metadata = TransferWgsCramV1Metadata(
      cramPath = Some(cramPath),
      cramSize = Some(1000L),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
          ClioCommand.queryWgsCramName,
          "--project",
          project
        )
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
        results <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
          ClioCommand.queryWgsCramName,
          "--project",
          project,
          "--sample-alias",
          sampleAlias
        )
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

      results <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--project",
        project,
        "--sample-alias",
        sampleAlias,
        "--include-deleted"
      )
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

  def testMoveCram(
    oldStyleCrai: Boolean = false,
    changeBasename: Boolean = false
  ): Future[Assertion] = {

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cramContents = s"$randomId --- I am a dummy cram --- $randomId"
    val craiContents = s"$randomId --- I am a dummy crai --- $randomId"
    val alignmentMetricsContents =
      s"$randomId --- I am dummy alignment metrics --- $randomId"
    val fingerprintMetricsContents =
      s"$randomId --- I am dummy fingerprinting metrics --- $randomId"

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName =
      s"${if (oldStyleCrai) sample else cramName}${WgsCramExtensions.CraiExtensionAddition}"
    val alignmentMetricsName = s"$randomId.metrics"
    val fingerprintMetricsName = s"$randomId.metrics"

    val rootSource =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramSource = rootSource.resolve(cramName)
    val craiSource = rootSource.resolve(craiName)
    val alignmentMetricsSource = rootSource.resolve(alignmentMetricsName)
    val fingerprintMetricsSource = rootSource.resolve(fingerprintMetricsName)

    val endBasename = if (changeBasename) randomId else sample

    val rootDestination = rootSource.getParent.resolve(s"moved/$randomId/")
    val cramDestination =
      rootDestination.resolve(s"$endBasename${WgsCramExtensions.CramExtension}")
    val craiDestination =
      rootDestination.resolve(s"$endBasename${WgsCramExtensions.CraiExtension}")
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

    val args = Seq.concat(
      Seq(
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
      ),
      if (changeBasename) {
        Seq("--new-basename", endBasename)
      } else {
        Seq.empty
      }
    )

    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runClient(ClioCommand.moveWgsCramName, args: _*)
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

  it should "move the cram and crai together in GCP" in testMoveCram()

  it should "fixup the crai extension on move" in testMoveCram(
    oldStyleCrai = true
  )

  it should "support changing the cram and crai basename on move" in testMoveCram(
    changeBasename = true
  )

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

  it should "not move wgs-crams with no registered files" in {
    val key = TransferWgsCramV1Key(
      Location.GCP,
      s"project$randomId",
      s"sample$randomId",
      1
    )
    runUpsertCram(key, TransferWgsCramV1Metadata()).flatMap { _ =>
      recoverToExceptionIf[Exception] {
        runClient(
          ClioCommand.moveWgsCramName,
          "--location",
          key.location.entryName,
          "--project",
          key.project,
          "--sample-alias",
          key.sampleAlias,
          "--version",
          key.version.toString,
          "--destination",
          "gs://some-destination/"
        )
      }.map {
        _.getMessage should include("Nothing to move")
      }
    }
  }

  def testDeleteCram(existingNote: Option[String] = None): Future[Assertion] = {
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
    val cramPath =
      storageDir.resolve(s"$randomId${WgsCramExtensions.CramExtension}")
    val craiPath =
      storageDir.resolve(s"$randomId${WgsCramExtensions.CraiExtensionAddition}")
    val metrics1Path = storageDir.resolve(s"$randomId.metrics")
    val metrics2Path = storageDir.resolve(s"$randomId.metrics")

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata =
      TransferWgsCramV1Metadata(
        cramPath = Some(cramPath.toUri),
        craiPath = Some(craiPath.toUri),
        alignmentSummaryMetricsPath = Some(metrics1Path.toUri),
        fingerprintingSummaryMetricsPath = Some(metrics1Path.toUri),
        notes = existingNote
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
      outputs <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
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
    } yield {
      Seq(cramPath, craiPath).foreach {
        Files.exists(_) should be(false)
      }
      Seq((metrics1Path, metrics1Contents), (metrics2Path, metrics2Contents)).foreach {
        case (path, contents) => {
          Files.exists(path) should be(true)
          new String(Files.readAllBytes(path)) should be(contents)
        }
      }

      outputs should have length 1
      val output = outputs.head
      output.notes should be(
        existingNote
          .map(existing => s"$existing\n$deleteNote")
          .orElse(Some(deleteNote))
      )
      output.documentStatus should be(Some(DocumentStatus.Deleted))
    }

    result.andThen[Unit] {
      case _ => {
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Seq(cramPath, craiPath, metrics1Path, metrics2Path)
          .map(Files.deleteIfExists)
      }
    }
  }

  it should "delete crams in GCP along with their crais, but not their metrics" in testDeleteCram()

  it should "preserve existing notes when deleting wgs-crams" in testDeleteCram(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

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
    val id = randomId
    val project = s"project$id"
    val sample = s"sample$id"
    val version = 3

    val cramContents = s"$id --- I am a dummy cram --- $id"
    val craiContents = s"$id --- I am a dummy crai --- $id"
    val md5Contents = randomId

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName = s"$cramName${WgsCramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${WgsCramExtensions.Md5ExtensionAddition}"

    val rootSource =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramSource = rootSource.resolve(cramName)
    val craiSource = rootSource.resolve(craiName)

    val prefix = "new_basename_"
    val newBasename = s"$prefix$sample"
    val rootDestination = rootSource.getParent.resolve(s"moved/$id/")
    val cramDestination = rootDestination.resolve(s"$prefix$cramName")
    val craiDestination = rootDestination.resolve(s"$prefix$craiName")
    val md5Destination = rootDestination.resolve(s"$prefix$md5Name")

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata = TransferWgsCramV1Metadata(
      cramPath = Some(cramSource.toUri),
      craiPath = Some(craiSource.toUri),
      cramMd5 = Some(Symbol(md5Contents))
    )

    val workspaceName = s"$id-TestWorkspace-$id"

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
        rootDestination.toUri.toString,
        "--new-basename",
        newBasename
      )
      outputs <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--workspace-name",
        workspaceName
      )
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
            documentStatus = Some(DocumentStatus.Normal)
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

  it should "not fail delivery if the cram is already in its target location" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cramContents = s"$randomId --- I am a dummy cram --- $randomId"
    val craiContents = s"$randomId --- I am a dummy crai --- $randomId"
    val md5Contents = randomId

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName = s"$cramName${WgsCramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${WgsCramExtensions.Md5ExtensionAddition}"

    val rootSource =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramSource = rootSource.resolve(cramName)
    val craiSource = rootSource.resolve(craiName)

    val md5Destination = rootSource.resolve(md5Name)

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
        rootSource.toUri.toString
      )
      outputs <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(cramSource, craiSource, md5Destination).foreach(
        Files.exists(_) should be(true)
      )

      Seq(
        (cramSource, cramContents),
        (craiSource, craiContents),
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
            cramPath = Some(cramSource.toUri),
            craiPath = Some(craiSource.toUri),
            cramMd5 = Some(Symbol(md5Contents)),
            documentStatus = Some(DocumentStatus.Normal)
          )
        )
      }
    }

    result.andThen {
      case _ => {
        val _ =
          Seq(cramSource, craiSource, md5Destination).map(Files.deleteIfExists)
      }
    }
  }

  it should "respect user-set regulatory designation for crams" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val md5Contents = randomId

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName = s"$cramName${WgsCramExtensions.CraiExtensionAddition}"

    val rootSource =
      rootTestStorageDir.resolve(s"cram/$project/$sample/v$version/")
    val cramSource = rootSource.resolve(cramName)
    val craiSource = rootSource.resolve(craiName)

    val key = TransferWgsCramV1Key(Location.GCP, project, sample, version)
    val metadata = TransferWgsCramV1Metadata(
      cramPath = Some(cramSource.toUri),
      craiPath = Some(craiSource.toUri),
      cramMd5 = Some(Symbol(md5Contents)),
      regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
          ClioCommand.queryWgsCramName,
          "--project",
          project
        )
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
        Some(RegulatoryDesignation.ClinicalDiagnostics)
      )
    }
  }

  it should "not overwrite existing regulatory designation on cram delivery" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cramContents = s"$randomId --- I am a dummy cram --- $randomId"
    val craiContents = s"$randomId --- I am a dummy crai --- $randomId"
    val md5Contents = randomId

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName = s"$cramName${WgsCramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${WgsCramExtensions.Md5ExtensionAddition}"

    val cramRegulatoryDesignation =
      Some(RegulatoryDesignation.ClinicalDiagnostics)

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
      cramMd5 = Some(Symbol(md5Contents)),
      regulatoryDesignation = cramRegulatoryDesignation
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
      outputs <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
        ClioCommand.queryWgsCramName,
        "--workspace-name",
        workspaceName
      )
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
            regulatoryDesignation = cramRegulatoryDesignation
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

  it should "fail delivery if the underlying move fails" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val md5Contents = randomId

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName = s"$cramName${WgsCramExtensions.CraiExtensionAddition}"

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
        outputs <- runClientGetJsonAs[Seq[TransferWgsCramV1QueryOutput]](
          ClioCommand.queryWgsCramName,
          "--workspace-name",
          workspaceName
        )
      } yield {
        // The CLP shouldn't have tried to upsert the workspace name.
        outputs shouldBe empty
      }
    }
  }
}
