package org.broadinstitute.clio.integrationtest.tests

import java.net.URI
import java.nio.file.Files

import com.sksamuel.elastic4s.IndexAndType
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentGvcf,
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryOutput
}
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation,
  UpsertId
}
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests of Clio's gvcf functionality. */
trait GvcfTests { self: BaseIntegrationSpec =>

  def runUpsertGvcf(
    key: TransferGvcfV1Key,
    metadata: TransferGvcfV1Metadata,
    forceUpdate: Boolean = true
  ): Future[UpsertId] = {
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
      tmpMetadata.toString,
      if(forceUpdate)"--force-update" else ""
    ).mapTo[UpsertId]
  }

  it should "create the expected gvcf mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

    val expected = ElasticsearchIndex[DocumentGvcf]
    val getRequest = getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
  }

  it should "report the expected JSON schema for gvcf" in {
    runClient(ClioCommand.getGvcfSchemaName)
      .map(_ should be(GvcfIndex.jsonSchema))
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
      project = "test project",
      sampleAlias = s"someAlias $randomId",
      version = 2,
      documentStatus = Some(DocumentStatus.Normal),
      gvcfPath = Some(URI.create(s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"))
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
      TransferGvcfV1Metadata(gvcfPath = expected.gvcfPath)
    )
    it should s"handle upserts and queries for gvcf location $location" in {
      for {
        returnedUpsertId <- responseFuture
        outputs <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
          ClioCommand.queryGvcfName,
          "--sample-alias",
          expected.sampleAlias
        )
      } yield {
        outputs should be(Seq(expected))

        val storedDocument = getJsonFrom[DocumentGvcf](returnedUpsertId)
        storedDocument.location should be(expected.location)
        storedDocument.project should be(expected.project)
        storedDocument.sampleAlias should be(expected.sampleAlias)
        storedDocument.version should be(expected.version)
        storedDocument.gvcfPath should be(expected.gvcfPath)
      }
    }
  }

  it should "assign different upsertIds to different gvcf upserts" in {
    val upsertKey = TransferGvcfV1Key(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )

    val gvcfUri1 =
      Some(URI.create(s"gs://path/gvcf1${GvcfExtensions.GvcfExtension}"))
    val gvcfUri2 =
      Some(URI.create(s"gs://path/gvcf2${GvcfExtensions.GvcfExtension}"))

    for {
      upsertId1 <- runUpsertGvcf(
        upsertKey,
        TransferGvcfV1Metadata(gvcfPath = gvcfUri1)
      )
      upsertId2 <- runUpsertGvcf(
        upsertKey,
        TransferGvcfV1Metadata(gvcfPath = gvcfUri2)
      )
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 =
        getJsonFrom[DocumentGvcf](upsertId1)
      storedDocument1.gvcfPath should be(gvcfUri1)

      val storedDocument2 =
        getJsonFrom[DocumentGvcf](upsertId2)
      storedDocument2.gvcfPath should be(gvcfUri2)

      storedDocument1.copy(upsertId = upsertId2, gvcfPath = gvcfUri2) should be(
        storedDocument2
      )
    }
  }

  it should "assign different upsertIds to equal gvcf upserts" in {
    val upsertKey = TransferGvcfV1Key(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )
    val upsertData =
      TransferGvcfV1Metadata(
        gvcfPath = Some(URI.create(s"gs://path/gvcf1${GvcfExtensions.GvcfExtension}"))
      )

    for {
      upsertId1 <- runUpsertGvcf(upsertKey, upsertData)
      upsertId2 <- runUpsertGvcf(upsertKey, upsertData)
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 = getJsonFrom[DocumentGvcf](upsertId1)
      val storedDocument2 = getJsonFrom[DocumentGvcf](upsertId2)
      storedDocument1.copy(upsertId = upsertId2) should be(storedDocument2)
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
            gvcfPath = Some(
              URI.create(s"gs://path/gvcf${GvcfExtensions.GvcfExtension}")
            ),
            contamination = Some(.65f)
          )
          runUpsertGvcf(key, data)
      }
    }

    for {
      _ <- upserts
      projectResults <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
        ClioCommand.queryGvcfName,
        "--project",
        project
      )
      sampleResults <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
        ClioCommand.queryGvcfName,
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

  it should "handle updates to gvcf metadata" in {
    val project = s"testProject$randomId"
    val key =
      TransferGvcfV1Key(Location.GCP, project, s"testSample$randomId", 1)
    val gvcfPath = URI.create(s"gs://path/gvcf${GvcfExtensions.GvcfExtension}")
    val metadata = TransferGvcfV1Metadata(
      gvcfPath = Some(gvcfPath),
      contamination = Some(.75f),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
          ClioCommand.queryGvcfName,
          "--project",
          project
        )
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
        gvcfPath = Some(
          URI.create(
            s"gs://gvcf/$sampleAlias.$version.${GvcfExtensions.GvcfExtension}"
          )
        )
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
        results <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
          ClioCommand.queryGvcfName,
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
      _ <- runUpsertGvcf(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
        ClioCommand.queryGvcfName,
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

  it should "respect user-set regulatory designation for gvcfs" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cloudPath = rootTestStorageDir.resolve(
      s"gvcf/$project/$sample/v$version/$randomId${GvcfExtensions.GvcfExtension}"
    )

    val key = TransferGvcfV1Key(Location.GCP, project, sample, version)
    val metadata =
      TransferGvcfV1Metadata(
        gvcfPath = Some(cloudPath.toUri),
        regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
      )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
          ClioCommand.queryGvcfName,
          "--project",
          project
        )
      } yield {
        results should have length 1
        results.head
      }
    }

    for {
      _ <- runUpsertGvcf(key, metadata)
      result <- query
    } yield {
      result.regulatoryDesignation should be(
        Some(RegulatoryDesignation.ClinicalDiagnostics)
      )
    }
  }

  it should "preserve any existing regulatory designation for gvcfs" in {
    val expectedOutput = TransferGvcfV1QueryOutput(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 3,
      gvcfPath = Some(URI.create(s"gs://gvcf/$randomId${GvcfExtensions.GvcfExtension}")),
      regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val key = TransferGvcfV1Key(
      expectedOutput.location,
      expectedOutput.project,
      expectedOutput.sampleAlias,
      expectedOutput.version
    )
    val firstMetadata = TransferGvcfV1Metadata(
      regulatoryDesignation = expectedOutput.regulatoryDesignation
    )
    val secondMetadata =
      TransferGvcfV1Metadata(gvcfPath = expectedOutput.gvcfPath)

    for {
      _ <- runUpsertGvcf(key, firstMetadata)
      _ <- runUpsertGvcf(key, secondMetadata)
      queryOutput <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
        ClioCommand.queryGvcfName,
        "--project",
        key.project,
        "--sample-alias",
        key.sampleAlias,
        "--version",
        key.version.toString
      )
    } yield {
      queryOutput should be(Seq(expectedOutput))
    }
  }

  def testMoveGvcf(
    srcIsDest: Boolean = false,
    gvcfInDest: Boolean = false
  ): Future[Assertion] = {

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val gvcfContents = s"$randomId --- I am a dummy gvcf --- $randomId"
    val indexContents = s"$randomId --- I am a dummy index --- $randomId"
    val summaryMetricsContents =
      s"$randomId --- I am dummy summary metrics --- $randomId"
    val detailMetricsContents =
      s"$randomId --- I am dummy detail metrics --- $randomId"

    val gvcfName = s"$randomId${GvcfExtensions.GvcfExtension}"
    val indexName = s"$randomId${GvcfExtensions.IndexExtension}"
    val summaryMetricsName =
      s"$randomId${GvcfExtensions.SummaryMetricsExtension}"
    val detailMetricsName = s"$randomId${GvcfExtensions.DetailMetricsExtension}"

    val rootSource =
      rootTestStorageDir.resolve(s"gvcf/$project/$sample/v$version/")
    val rootDestination = if (srcIsDest) {
      rootSource
    } else {
      rootSource.getParent.resolve(s"moved/$randomId/")
    }

    val gvcfSource = if (gvcfInDest) {
      rootDestination.resolve(gvcfName)
    } else {
      rootSource.resolve(gvcfName)
    }
    val indexSource = rootSource.resolve(indexName)
    val summaryMetricsSource = rootSource.resolve(summaryMetricsName)
    val detailMetricsSource = rootSource.resolve(detailMetricsName)

    val gvcfDestination = rootDestination.resolve(gvcfName)
    val indexDestination = rootDestination.resolve(indexName)
    val summaryMetricsDestination = rootDestination.resolve(summaryMetricsName)
    val detailMetricsDestination = rootDestination.resolve(detailMetricsName)

    val key = TransferGvcfV1Key(Location.GCP, project, sample, version)
    val metadata = TransferGvcfV1Metadata(
      gvcfPath = Some(gvcfSource.toUri),
      gvcfIndexPath = Some(indexSource.toUri),
      gvcfSummaryMetricsPath = Some(summaryMetricsSource.toUri),
      gvcfDetailMetricsPath = Some(detailMetricsSource.toUri)
    )

    val _ = Seq(
      (gvcfSource, gvcfContents),
      (indexSource, indexContents),
      (summaryMetricsSource, summaryMetricsContents),
      (detailMetricsSource, detailMetricsContents)
    ).map {
      case (source, contents) => Files.write(source, contents.getBytes)
    }
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
        rootDestination.toUri.toString
      )
    } yield {
      Files.exists(gvcfSource) should be(srcIsDest || gvcfInDest)
      Seq(indexSource, summaryMetricsSource, detailMetricsSource)
        .foreach(Files.exists(_) should be(srcIsDest))
      Seq(
        gvcfDestination,
        indexDestination,
        summaryMetricsDestination,
        detailMetricsDestination
      ).foreach(Files.exists(_) should be(true))
      Seq(
        (gvcfDestination, gvcfContents),
        (indexDestination, indexContents),
        (summaryMetricsDestination, summaryMetricsContents),
        (detailMetricsDestination, detailMetricsContents)
      ).foreach {
        case (destination, contents) =>
          new String(Files.readAllBytes(destination)) should be(contents)
      }
      succeed
    }

    result.andThen {
      case _ => {
        val _ = Seq(
          gvcfSource,
          gvcfDestination,
          indexSource,
          indexDestination,
          summaryMetricsSource,
          summaryMetricsDestination,
          detailMetricsSource,
          detailMetricsDestination
        ).map(Files.deleteIfExists)
      }
    }
  }

  it should "move the gvcf, index, and metrics files together in GCP" in testMoveGvcf()

  it should "not delete gvcf files when moving and source == destination" in testMoveGvcf(
    srcIsDest = true
  )

  it should "move gvcf files when one file is already in the source directory" in testMoveGvcf(
    gvcfInDest = true
  )

  it should "not move gvcfs without a destination" in {
    recoverToExceptionIf[Exception] {
      runClient(
        ClioCommand.moveGvcfName,
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

  def testDeleteGvcf(existingNote: Option[String] = None): Future[Assertion] = {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val gvcfContents = s"$randomId --- I am a gvcf fated to die --- $randomId"
    val indexContents =
      s"$randomId --- I am an index fated to die --- $randomId"
    val summaryMetricsContents =
      s"$randomId --- I am an immortal summary metrics file --- $randomId"
    val detailMetricsContents =
      s"$randomId --- I am an immortal detail metrics file --- $randomId"

    val deleteNote =
      s"$randomId --- Deleted by the integration tests --- $randomId"

    val storageDir =
      rootTestStorageDir.resolve(s"gvcf/$project/$sample/v$version/")
    val gvcfPath =
      storageDir.resolve(s"$randomId${GvcfExtensions.GvcfExtension}")
    val indexPath =
      storageDir.resolve(s"$randomId${GvcfExtensions.IndexExtension}")
    val summaryMetricsPath =
      storageDir.resolve(s"$randomId${GvcfExtensions.SummaryMetricsExtension}")
    val detailMetricsPath =
      storageDir.resolve(s"$randomId${GvcfExtensions.DetailMetricsExtension}")

    val key = TransferGvcfV1Key(Location.GCP, project, sample, version)
    val metadata =
      TransferGvcfV1Metadata(
        gvcfPath = Some(gvcfPath.toUri),
        gvcfIndexPath = Some(indexPath.toUri),
        gvcfSummaryMetricsPath = Some(summaryMetricsPath.toUri),
        gvcfDetailMetricsPath = Some(detailMetricsPath.toUri),
        notes = existingNote
      )

    val _ = Seq(
      (gvcfPath, gvcfContents),
      (indexPath, indexContents),
      (summaryMetricsPath, summaryMetricsContents),
      (detailMetricsPath, detailMetricsContents)
    ).map {
      case (path, contents) => Files.write(path, contents.getBytes)
    }
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
      outputs <- runClientGetJsonAs[Seq[TransferGvcfV1QueryOutput]](
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
    } yield {
      Files.exists(gvcfPath) should be(false)
      Files.exists(indexPath) should be(false)
      Files.exists(summaryMetricsPath) should be(true)
      Files.exists(detailMetricsPath) should be(true)
      new String(Files.readAllBytes(summaryMetricsPath)) should be(
        summaryMetricsContents
      )
      new String(Files.readAllBytes(detailMetricsPath)) should be(
        detailMetricsContents
      )

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
        val _ = Seq(gvcfPath, indexPath, summaryMetricsPath, detailMetricsPath)
          .map(Files.deleteIfExists)
      }
    }
  }

  it should "delete gvcfs and their indexes in GCP, but not their metrics" in testDeleteGvcf()

  it should "preserve existing notes when deleting gvcfs" in testDeleteGvcf(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  it should "not delete gvcfs without a note" in {
    recoverToExceptionIf[Exception] {
      runClient(
        ClioCommand.deleteGvcfName,
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
}
