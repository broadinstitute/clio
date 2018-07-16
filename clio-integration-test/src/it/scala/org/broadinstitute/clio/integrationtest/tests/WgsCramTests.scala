package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.transfer.model.wgscram._
import org.broadinstitute.clio.util.model._
import org.scalatest.Assertion

import scala.concurrent.Future

//TODO This should be deleted once we're using the new Cram API in all places
/** Tests of Clio's wgs-cram functionality. */
trait WgsCramTests { self: BaseIntegrationSpec =>
  import org.broadinstitute.clio.JsonUtils.JsonOps

  def runUpsertCram(
    key: CramKey,
    metadata: CramMetadata,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runDecode[UpsertId](
      ClioCommand.addWgsCramName,
      Seq(
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
        if (force) "--force" else ""
      ).filter(_.nonEmpty): _*
    )
  }

  it should "create the expected wgs-cram mapping in elasticsearch" in {
    import ElasticsearchUtil.HttpClientOps
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val expected = ElasticsearchIndex.Cram
    val getRequest = getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
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
    it should s"handle upserts and queries for wgs-cram location $location" in {
      val key = CramKey(
        location = location,
        project = "test project",
        sampleAlias = s"someAlias $randomId",
        version = 2,
        dataType = DataType.WGS
      )
      val metadata = CramMetadata(
        documentStatus = Some(DocumentStatus.Normal),
        cramPath = Some(URI.create(s"gs://path/cram${CramExtensions.CramExtension}"))
      )
      val expected = expectedMerge(key, metadata)

      for {
        returnedUpsertId <- runUpsertCram(key, metadata.copy(documentStatus = None))
        outputs <- runCollectJson(
          ClioCommand.queryWgsCramName,
          "--sample-alias",
          key.sampleAlias
        )
      } yield {
        outputs should contain only expected
        val storedDocument = getJsonFrom(returnedUpsertId)(ElasticsearchIndex.Cram)
        storedDocument.mapObject(
          _.filterKeys(!ElasticsearchIndex.BookkeepingNames.contains(_))
        ) should be(expected)
      }
    }
  }

  it should "assign different upsertIds to different wgs-cram upserts" in {
    val upsertKey = CramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )

    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        CramMetadata(
          cramPath = Some(
            URI.create(s"gs://path/cram1${CramExtensions.CramExtension}")
          )
        )
      )
      upsertId2 <- runUpsertCram(
        upsertKey,
        CramMetadata(
          cramPath = Some(
            URI.create(s"gs://path/cram2${CramExtensions.CramExtension}")
          )
        )
      )
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Cram)
      storedDocument1.unsafeGet[URI]("cram_path") should be(
        URI.create(s"gs://path/cram1${CramExtensions.CramExtension}")
      )

      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Cram)
      storedDocument2.unsafeGet[URI]("cram_path") should be(
        URI.create(s"gs://path/cram2${CramExtensions.CramExtension}")
      )

      storedDocument1.deepMerge {
        Json.obj(
          ElasticsearchIndex.UpsertIdElasticsearchName -> upsertId2.asJson,
          "cram_path" -> s"gs://path/cram2${CramExtensions.CramExtension}".asJson
        )
      } should be(storedDocument2)
    }
  }

  it should "assign different upsertIds to equal wgs-cram upserts" in {
    val upsertKey = CramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    val upsertData = CramMetadata(
      cramPath = Some(URI.create(s"gs://path/cram1${CramExtensions.CramExtension}"))
    )

    for {
      upsertId1 <- runUpsertCram(upsertKey, upsertData)
      upsertId2 <- runUpsertCram(upsertKey, upsertData)
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Cram)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Cram)
      storedDocument1.mapObject(
        _.add(ElasticsearchIndex.UpsertIdElasticsearchName, upsertId2.asJson)
      ) should be(storedDocument2)
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
          val key = WgsCramKey(location, project, sample, version)
          val data = CramMetadata(
            cramPath = Some(
              URI.create(s"gs://path/cram${CramExtensions.CramExtension}")
            ),
            cramSize = Some(1000L)
          )
          runUpsertCram(key, data)
      }
    }

    for {
      _ <- upserts
      projectResults <- runCollectJson(
        ClioCommand.queryWgsCramName,
        "--project",
        project
      )
      sampleResults <- runCollectJson(
        ClioCommand.queryWgsCramName,
        "--sample-alias",
        samples.head
      )
    } yield {
      projectResults should have length 3
      projectResults.foldLeft(succeed) { (_, result) =>
        result.unsafeGet[String]("project") should be(project)
      }
      sampleResults should have length 2
      sampleResults.foldLeft(succeed) { (_, result) =>
        result.unsafeGet[String]("sample_alias") should be(samples.head)
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
          val key = WgsCramKey(location, project, sample, version)
          val data = CramMetadata(
            cramPath = Some(
              URI.create(s"gs://path/cram${CramExtensions.CramExtension}")
            ),
            cramSize = Some(1000L)
          )
          runUpsertCram(key, data)
      }
    }

    for {
      _ <- upserts
      prefixResults <- runCollectJson(
        ClioCommand.queryWgsCramName,
        "--sample-alias",
        prefix
      )
      suffixResults <- runCollectJson(
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
    val key = WgsCramKey(Location.GCP, project, s"testSample$randomId", 1)
    val cramPath = URI.create(s"gs://path/cram${CramExtensions.CramExtension}")
    val cramSize = 1000L
    val initialNotes = "Breaking news"
    val metadata = CramMetadata(
      cramPath = Some(cramPath),
      cramSize = Some(cramSize),
      notes = Some(initialNotes)
    )

    def query = {
      for {
        results <- runCollectJson(
          ClioCommand.queryWgsCramName,
          "--project",
          project
        )
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = CramMetadata(
      cramSize = metadata.cramSize,
      cramPath = metadata.cramPath
    )

    for {
      _ <- runUpsertCram(key, upsertData)
      original <- query
      _ = original.unsafeGet[URI]("cram_path") should be(cramPath)
      _ = original.unsafeGet[Long]("cram_size") should be(cramSize)
      _ = original.unsafeGet[Option[String]]("notes") should be(None)

      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertCram(key, upsertData2)
      withNotes <- query
      _ = withNotes.unsafeGet[URI]("cram_path") should be(cramPath)
      _ = withNotes.unsafeGet[Long]("cram_size") should be(cramSize)
      _ = withNotes.unsafeGet[String]("notes") should be(initialNotes)

      _ <- runUpsertCram(
        key,
        upsertData2.copy(cramSize = Some(2000L), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.unsafeGet[URI]("cram_path") should be(cramPath)
      emptyNotes.unsafeGet[Long]("cram_size") should be(2000L)
      emptyNotes.unsafeGet[String]("notes") should be("")
    }
  }

  it should "show deleted wgs-cram records on queryAll, but not query" in {
    val project = "testProject" + randomId
    val sampleAlias = "sample688." + randomId

    val keysWithMetadata = (1 to 3).map { version =>
      val upsertKey = CramKey(
        location = Location.GCP,
        project = project,
        sampleAlias = sampleAlias,
        version = version,
        dataType = DataType.WGS
      )
      val upsertMetadata = CramMetadata(
        cramPath = Some(URI.create(s"gs://cram/$sampleAlias.$version"))
      )
      (upsertKey, upsertMetadata)
    }
    val (deleteKey, deleteData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (v1Key, metadata) => runUpsertCram(v1Key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        results <- runCollectJson(
          ClioCommand.queryWgsCramName,
          "--project",
          project,
          "--sample-alias",
          sampleAlias
        )
      } yield {
        results.length should be(expectedLength)
        results.foreach { result =>
          result.unsafeGet[String]("project") should be(project)
          result.unsafeGet[String]("sample_alias") should be(sampleAlias)
          result.unsafeGet[DocumentStatus]("document_status") should be(
            DocumentStatus.Normal
          )
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

      results <- runCollectJson(
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
        result.unsafeGet[String]("project") should be(project)
        result.unsafeGet[String]("sample_alias") should be(sampleAlias)

        val resultKey = CramKey(
          location = result.unsafeGet[Location]("location"),
          project = result.unsafeGet[String]("project"),
          sampleAlias = result.unsafeGet[String]("sample_alias"),
          version = result.unsafeGet[Int]("version"),
          dataType = result.unsafeGet[DataType]("data_type")
        )

        result.unsafeGet[DocumentStatus]("document_status") should be {
          if (resultKey == deleteKey) DocumentStatus.Deleted else DocumentStatus.Normal
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
    val analysisFilesContents =
      s"$randomId --- I am dummy analysis files file --- $randomId"
    val alignmentSummaryMetricsContents =
      s"$randomId --- I am dummy alignment summary metrics --- $randomId"
    val cramValidationReportContents =
      s"$randomId --- I am a dummy cram validation report --- $randomId"
    val crosscheckContents =
      s"$randomId --- I am a dummy crosscheck file --- $randomId"
    val fingerprintingSummaryMetricsContents =
      s"$randomId --- I am dummy fingerprinting summary metrics --- $randomId"
    val fingerprintingDetailMetricsContents =
      s"$randomId --- I am dummy fingerprinting detail metrics --- $randomId"
    val preAdapterSummaryMetricsContents =
      s"$randomId --- I am dummy pre-adapter summary metrics --- $randomId"
    val preAdapterDetailMetricsContents =
      s"$randomId --- I am dummy pre-adapter detail metrics --- $randomId"
    val preBqsrSelfSmContents =
      s"$randomId --- I am a dumy pre-BQSR Self file --- $randomId"

    val cramName = s"$sample${CramExtensions.CramExtension}"
    val craiName =
      s"${if (oldStyleCrai) sample else cramName}${CramExtensions.CraiExtensionAddition}"

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName
    val analysisFilesSource = rootSource / s"$sample${CramExtensions.AnalysisFilesTxtExtension}"
    val alignmentSummaryMetricsSource = rootSource / s"$sample${CramExtensions.AlignmentSummaryMetricsExtension}"
    val cramValidationReportSource = rootSource / s"$sample${CramExtensions.CramValidationReportExtension}"
    val crosscheckSource = rootSource / s"$sample${CramExtensions.CrossCheckExtension}"
    val fingerprintSummaryMetricsSource = rootSource / s"$sample${CramExtensions.FingerprintingSummaryMetricsExtension}"
    val fingerprintDetailMetricsSource = rootSource / s"$sample${CramExtensions.FingerprintingDetailMetricsExtension}"
    val preAdapterSummaryMetricsSource = rootSource / s"$sample${CramExtensions.PreAdapterSummaryMetricsExtension}"
    val preAdapterDetailMetricsSource = rootSource / s"$sample${CramExtensions.PreAdapterDetailMetricsExtension}"
    val preBqsrSelfSmSource = rootSource / s"$sample${CramExtensions.PreBqsrSelfSMExtension}"

    val endBasename = if (changeBasename) randomId else sample

    val rootDestination = rootSource.parent / s"moved/$randomId/"
    val cramDestination = rootDestination / s"$endBasename${CramExtensions.CramExtension}"
    val craiDestination = rootDestination / s"$endBasename${CramExtensions.CraiExtension}"
    val analysisFilesDestination = rootDestination / s"$endBasename${CramExtensions.AnalysisFilesTxtExtension}"
    val alignmentSummaryMetricsDestination = rootDestination / s"$endBasename${CramExtensions.AlignmentSummaryMetricsExtension}"
    val cramValidationReportDestination = rootDestination / s"$endBasename${CramExtensions.CramValidationReportExtension}"
    val crosscheckDestination = rootDestination / s"$endBasename${CramExtensions.CrossCheckExtension}"
    val fingerprintSummaryMetricsDestination = rootDestination / s"$endBasename${CramExtensions.FingerprintingSummaryMetricsExtension}"
    val fingerprintDetailMetricsDestination = rootDestination / s"$endBasename${CramExtensions.FingerprintingDetailMetricsExtension}"
    val preAdapterSummaryMetricsDestination = rootDestination / s"$endBasename${CramExtensions.PreAdapterSummaryMetricsExtension}"
    val preAdapterDetailMetricsDestination = rootDestination / s"$endBasename${CramExtensions.PreAdapterDetailMetricsExtension}"
    val preBqsrSelfSmDestination = rootDestination / s"$endBasename${CramExtensions.PreBqsrSelfSMExtension}"

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      analysisFilesTxtPath = Some(analysisFilesSource.uri),
      alignmentSummaryMetricsPath = Some(alignmentSummaryMetricsSource.uri),
      cramValidationReportPath = Some(cramValidationReportSource.uri),
      crosscheckPath = Some(crosscheckSource.uri),
      fingerprintingSummaryMetricsPath = Some(fingerprintSummaryMetricsSource.uri),
      fingerprintingDetailMetricsPath = Some(fingerprintDetailMetricsSource.uri),
      preAdapterSummaryMetricsPath = Some(preAdapterSummaryMetricsSource.uri),
      preAdapterDetailMetricsPath = Some(preAdapterDetailMetricsSource.uri),
      preBqsrSelfSmPath = Some(preBqsrSelfSmSource.uri)
    )

    val _ = Seq(
      (cramSource, cramContents),
      (craiSource, craiContents),
      (analysisFilesSource, analysisFilesContents),
      (alignmentSummaryMetricsSource, alignmentSummaryMetricsContents),
      (cramValidationReportSource, cramValidationReportContents),
      (crosscheckSource, crosscheckContents),
      (fingerprintSummaryMetricsSource, fingerprintingSummaryMetricsContents),
      (fingerprintDetailMetricsSource, fingerprintingDetailMetricsContents),
      (preAdapterSummaryMetricsSource, preAdapterSummaryMetricsContents),
      (preAdapterDetailMetricsSource, preAdapterDetailMetricsContents),
      (preBqsrSelfSmSource, preBqsrSelfSmContents)
    ).map {
      case (source, contents) => source.write(contents)
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
        rootDestination.uri.toString
      ),
      if (changeBasename) {
        Seq("--new-basename", endBasename)
      } else {
        Seq.empty
      }
    )

    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runIgnore(ClioCommand.moveWgsCramName, args: _*)
    } yield {
      Seq(
        cramSource,
        craiSource,
        analysisFilesSource,
        alignmentSummaryMetricsSource,
        cramValidationReportSource,
        crosscheckSource,
        fingerprintSummaryMetricsSource,
        fingerprintDetailMetricsSource,
        preAdapterSummaryMetricsSource,
        preAdapterDetailMetricsSource,
        preBqsrSelfSmSource
      ).foreach(_ shouldNot exist)

      Seq(
        cramDestination,
        craiDestination,
        analysisFilesDestination,
        alignmentSummaryMetricsDestination,
        cramValidationReportDestination,
        crosscheckDestination,
        fingerprintSummaryMetricsDestination,
        fingerprintDetailMetricsDestination,
        preAdapterSummaryMetricsDestination,
        preAdapterDetailMetricsDestination,
        preBqsrSelfSmDestination
      ).foreach(_ should exist)

      // We don't deliver fingerprinting metrics for now because they're based on unpublished research.
//      fingerprintMetricsSource should exist
//      fingerprintMetricsDestination shouldNot exist

      Seq(
        (cramDestination, cramContents),
        (craiDestination, craiContents),
        (analysisFilesDestination, analysisFilesContents),
        (alignmentSummaryMetricsDestination, alignmentSummaryMetricsContents),
        (cramValidationReportDestination, cramValidationReportContents),
        (crosscheckDestination, crosscheckContents),
        (fingerprintSummaryMetricsDestination, fingerprintingSummaryMetricsContents),
        (fingerprintDetailMetricsDestination, fingerprintingDetailMetricsContents),
        (preAdapterSummaryMetricsDestination, preAdapterSummaryMetricsContents),
        (preAdapterDetailMetricsDestination, preAdapterDetailMetricsContents),
        (preBqsrSelfSmDestination, preBqsrSelfSmContents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
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
          analysisFilesSource,
          analysisFilesDestination,
          alignmentSummaryMetricsSource,
          alignmentSummaryMetricsDestination,
          cramValidationReportSource,
          cramValidationReportDestination,
          crosscheckSource,
          crosscheckDestination,
          fingerprintSummaryMetricsSource,
          fingerprintSummaryMetricsDestination,
          fingerprintDetailMetricsSource,
          fingerprintDetailMetricsDestination,
          preAdapterSummaryMetricsSource,
          preAdapterSummaryMetricsDestination,
          preAdapterDetailMetricsSource,
          preAdapterDetailMetricsDestination,
          preBqsrSelfSmSource,
          preBqsrSelfSmDestination
        ).map(_.delete(swallowIOExceptions = true))
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
      runDecode[UpsertId](
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
    val key = WgsCramKey(
      Location.GCP,
      s"project$randomId",
      s"sample$randomId",
      1
    )
    runUpsertCram(key, CramMetadata()).flatMap { _ =>
      recoverToExceptionIf[Exception] {
        runDecode[UpsertId](
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

  def testDeleteCram(
    existingNote: Option[String] = None,
    testNonExistingFile: Boolean = false,
    force: Boolean = false,
    workspaceName: Option[String] = None
  ): Future[Assertion] = {
    val deleteNote = s"$randomId --- Deleted by the integration tests --- $randomId"

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cramContents = s"$randomId --- I am a cram fated to die --- $randomId"
    val craiContents = s"$randomId --- I am an index fated to die --- $randomId"
    val metrics1Contents = s"$randomId --- I am an immortal metrics file --- $randomId"
    val metrics2Contents =
      s"$randomId --- I am a second immortal metrics file --- $randomId"

    val storageDir = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramPath = storageDir / s"$randomId${CramExtensions.CramExtension}"
    val craiPath = storageDir / s"$randomId${CramExtensions.CraiExtensionAddition}"
    val metrics1Path = storageDir / s"$randomId.metrics"
    val metrics2Path = storageDir / s"$randomId.metrics"

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramPath.uri),
      craiPath = Some(craiPath.uri),
      alignmentSummaryMetricsPath = Some(metrics1Path.uri),
      fingerprintingSummaryMetricsPath = Some(metrics1Path.uri),
      notes = existingNote,
      workspaceName = workspaceName
    )

    val _ = if (!testNonExistingFile) {
      Seq(
        (cramPath, cramContents),
        (craiPath, craiContents),
        (metrics1Path, metrics1Contents),
        (metrics2Path, metrics2Contents)
      ).map {
        case (path, contents) => path.write(contents)
      }
    }

    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runIgnore(
        ClioCommand.deleteWgsCramName,
        Seq(
          "--location",
          Location.GCP.entryName,
          "--project",
          project,
          "--sample-alias",
          sample,
          "--version",
          version.toString,
          "--note",
          deleteNote,
          if (force) "--force" else ""
        ).filter(_.nonEmpty): _*
      )
      outputs <- runCollectJson(
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
      Seq(cramPath, craiPath).foreach(_ shouldNot exist)
      if (!testNonExistingFile) {
        Seq((metrics1Path, metrics1Contents), (metrics2Path, metrics2Contents)).foreach {
          case (path, contents) => {
            path should exist
            path.contentAsString should be(contents)
          }
        }
      }

      outputs should have length 1
      val output = outputs.head
      output.unsafeGet[String]("notes") should be(
        existingNote.fold(deleteNote)(existing => s"$existing\n$deleteNote")
      )
      output.unsafeGet[DocumentStatus]("document_status") should be(
        DocumentStatus.Deleted
      )
    }

    result.andThen[Unit] {
      case _ => {
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Seq(cramPath, craiPath, metrics1Path, metrics2Path)
          .map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "delete crams in GCP along with their crais, but not their metrics" in testDeleteCram()

  it should "preserve existing notes when deleting wgs-crams" in testDeleteCram(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  it should "not delete wgs-crams without a note" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
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

  it should "throw an exception deleting the associated md5 if the workspaceName is set and the md5 is missing" in {
    recoverToSucceededIf[Exception] {
      testDeleteCram(workspaceName = Some("testWorkspace"))
    }
  }

  it should "throw an exception when trying to delete a cram if a file does not exist" in {
    recoverToSucceededIf[Exception] {
      testDeleteCram(testNonExistingFile = true)
    }
  }

  it should "delete a cram if a file does not exist and force is true" in testDeleteCram(
    testNonExistingFile = true,
    force = true
  )

  it should "move files, generate an md5 file, and record the workspace name when delivering crams" in {
    val id = randomId
    val project = s"project$id"
    val sample = s"sample$id"
    val version = 3

    val cramContents = s"$id --- I am a dummy cram --- $id"
    val craiContents = s"$id --- I am a dummy crai --- $id"
    val md5Contents = randomId

    val cramName = s"$sample${CramExtensions.CramExtension}"
    val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${CramExtensions.Md5ExtensionAddition}"

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val prefix = "new_basename_"
    val newBasename = s"$prefix$sample"
    val rootDestination = rootSource.parent / s"moved/$id/"
    val cramDestination = rootDestination / s"$prefix$cramName"
    val craiDestination = rootDestination / s"$prefix$craiName"
    val md5Destination = rootDestination / s"$prefix$md5Name"

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents)),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$id-TestWorkspace-$id"

    val _ = Seq((cramSource, cramContents), (craiSource, craiContents)).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runIgnore(
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
        rootDestination.uri.toString,
        "--new-basename",
        newBasename
      )
      outputs <- runCollectJson(
        ClioCommand.queryWgsCramName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(cramSource, craiSource).foreach(_ shouldNot exist)

      Seq(cramDestination, craiDestination, md5Destination).foreach(_ should exist)

      Seq(
        (cramDestination, cramContents),
        (craiDestination, craiContents),
        (md5Destination, md5Contents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
      }

      outputs should contain only expectedMerge(
        key: CramKey,
        metadata.copy(
          workspaceName = Some(workspaceName),
          cramPath = Some(cramDestination.uri),
          craiPath = Some(craiDestination.uri)
        )
      )
    }

    result.andThen {
      case _ => {
        val _ = Seq(
          cramSource,
          cramDestination,
          craiSource,
          craiDestination,
          md5Destination
        ).map(_.delete(swallowIOExceptions = true))
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

    val cramName = s"$sample${CramExtensions.CramExtension}"
    val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${CramExtensions.Md5ExtensionAddition}"

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val md5Destination = rootSource / md5Name

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents)),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    val _ = Seq((cramSource, cramContents), (craiSource, craiContents)).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runIgnore(
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
        rootSource.uri.toString
      )
      outputs <- runCollectJson(
        ClioCommand.queryWgsCramName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(cramSource, craiSource, md5Destination).foreach(_ should exist)

      Seq(
        (cramSource, cramContents),
        (craiSource, craiContents),
        (md5Destination, md5Contents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
      }

      outputs should contain only expectedMerge(
        key: CramKey,
        metadata.copy(workspaceName = Some(workspaceName))
      )
    }

    result.andThen {
      case _ => {
        val _ = Seq(cramSource, craiSource, md5Destination)
          .map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "respect user-set regulatory designation for crams" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val md5Contents = randomId

    val cramName = s"$sample${CramExtensions.CramExtension}"
    val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents)),
      regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
    )

    def query = {
      for {
        results <- runCollectJson(
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
      result.unsafeGet[RegulatoryDesignation]("regulatory_designation") should be(
        RegulatoryDesignation.ClinicalDiagnostics
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

    val cramName = s"$sample${CramExtensions.CramExtension}"
    val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${CramExtensions.Md5ExtensionAddition}"

    val cramRegulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val rootDestination = rootSource.parent / s"moved/$randomId/"
    val cramDestination = rootDestination / cramName
    val craiDestination = rootDestination / craiName
    val md5Destination = rootDestination / md5Name

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents)),
      regulatoryDesignation = cramRegulatoryDesignation,
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    val _ = Seq((cramSource, cramContents), (craiSource, craiContents)).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runIgnore(
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
        rootDestination.uri.toString
      )
      outputs <- runCollectJson(
        ClioCommand.queryWgsCramName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(cramSource, craiSource).foreach(_ shouldNot exist)

      Seq(cramDestination, craiDestination, md5Destination).foreach(_ should exist)

      Seq(
        (cramDestination, cramContents),
        (craiDestination, craiContents),
        (md5Destination, md5Contents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
      }

      outputs should contain only expectedMerge(
        key: CramKey,
        metadata.copy(
          workspaceName = Some(workspaceName),
          cramPath = Some(cramDestination.uri),
          craiPath = Some(craiDestination.uri)
        )
      )
    }

    result.andThen {
      case _ => {
        val _ = Seq(
          cramSource,
          cramDestination,
          craiSource,
          craiDestination,
          md5Destination
        ).map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "fail delivery if the underlying move fails" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val md5Contents = randomId

    val cramName = s"$sample${CramExtensions.CramExtension}"
    val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val rootDestination = rootSource.parent / s"moved/$randomId/"

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents))
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    recoverToExceptionIf[Exception] {
      for {
        _ <- runUpsertCram(key, metadata)
        // Should fail because the source files don't exist.
        _ <- runIgnore(
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
          rootDestination.uri.toString
        )
      } yield {
        ()
      }
    }.flatMap { _ =>
      for {
        outputs <- runCollectJson(
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

  it should "upsert a new cram if force is false" in {
    val upsertKey = CramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        CramMetadata(notes = Some("I'm a note")),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Cram)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
    }
  }

  it should "allow an upsert that modifies values not already set or are unchanged if force is false" in {
    val upsertKey = CramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        CramMetadata(notes = Some("I'm a note")),
        force = false
      )
      upsertId2 <- runUpsertCram(
        upsertKey,
        CramMetadata(notes = Some("I'm a note"), cramSize = Some(12345)),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Cram)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Cram)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
      storedDocument2.unsafeGet[Long]("cram_size") should be(12345)
    }
  }

  it should "not allow an upsert that modifies values already set if force is false" in {
    val upsertKey = CramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        CramMetadata(notes = Some("I'm a note")),
        force = false
      )
      _ <- recoverToSucceededIf[Exception] {
        runUpsertCram(
          upsertKey,
          CramMetadata(
            notes = Some("I'm a different note")
          ),
          force = false
        )
      }
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Cram)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
    }
  }
}
