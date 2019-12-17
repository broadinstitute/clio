package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import better.files.File
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.transfer.model.cram._
import org.broadinstitute.clio.util.model._
import org.scalatest.Assertion

import scala.concurrent.Future
import scala.util.Random

/** Tests of Clio's cram functionality. */
trait CramTests { self: BaseIntegrationSpec =>
  import org.broadinstitute.clio.JsonUtils.JsonOps

  def runUpsertCram(
    key: CramKey,
    metadata: CramMetadata,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runDecode[UpsertId](
      ClioCommand.addCramName,
      Seq(
        "--location",
        key.location.entryName,
        "--project",
        key.project,
        "--data-type",
        key.dataType.entryName,
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

  it should "create the expected cram mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

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
    it should s"handle upserts and queries for cram location $location" in {
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
          ClioCommand.queryCramName,
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

  it should "assign different upsertIds to different cram upserts" in {
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

  it should "assign different upsertIds to equal cram upserts" in {
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

  it should "handle querying crams by sample and project" in {
    val location = Location.GCP
    val project = "testProject" + randomId

    val samples = {
      val sameId = "testSample" + randomId
      Seq(sameId, sameId, "testSample" + randomId)
    }

    val upserts = Future.sequence {
      samples.zip(1 to 3).map {
        case (sample, version) =>
          val key = CramKey(location, project, DataType.WGS, sample, version)
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
        ClioCommand.queryCramName,
        "--project",
        project
      )
      sampleResults <- runCollectJson(
        ClioCommand.queryCramName,
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

  it should "query crams by regulatory designation" in {
    val location = Location.GCP
    val project = "testProject" + randomId

    val samples = {
      val sameId = "testSample" + randomId
      Seq(sameId, sameId, "testSample" + randomId)
    }

    val upserts = Future.sequence {
      samples.zip(1 to 3).map {
        case (sample, version) =>
          val key = CramKey(location, project, DataType.WGS, sample, version)
          val data = CramMetadata(
            cramPath = Some(
              URI.create(s"gs://path/cram${CramExtensions.CramExtension}")
            ),
            cramSize = Some(1000L),
            regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
          )
          runUpsertCram(key, data)
      }
    }

    for {
      _ <- upserts
      results <- runCollectJson(
        ClioCommand.queryCramName,
        "--regulatory-designation",
        RegulatoryDesignation.ClinicalDiagnostics.entryName,
        "--project",
        project
      )
    } yield {
      results should have length 3
      results.foldLeft(succeed) { (_, result) =>
        result.unsafeGet[String]("project") should be(project)
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
          val key = CramKey(location, project, DataType.WGS, sample, version)
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
        ClioCommand.queryCramName,
        "--sample-alias",
        prefix
      )
      suffixResults <- runCollectJson(
        ClioCommand.queryCramName,
        "--sample-alias",
        suffix
      )
    } yield {
      prefixResults should have length 1
      suffixResults should have length 1
    }
  }

  it should "handle updates to cram metadata" in {
    val project = s"testProject$randomId"
    val key = CramKey(Location.GCP, project, DataType.WGS, s"testSample$randomId", 1)
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
          ClioCommand.queryCramName,
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

  def testQueryAll(documentStatus: DocumentStatus): Future[Assertion] = {
    val queryArg = documentStatus match {
      case DocumentStatus.Deleted  => "--include-deleted"
      case DocumentStatus.External => "--include-all"
      case _                       => ""
    }
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
    val (notNormalKey, notNormalData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (v1Key, metadata) => runUpsertCram(v1Key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        results <- runCollectJson(
          ClioCommand.queryCramName,
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
        notNormalKey,
        notNormalData.copy(documentStatus = Some(documentStatus))
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runCollectJson(
        ClioCommand.queryCramName,
        "--project",
        project,
        "--sample-alias",
        sampleAlias,
        queryArg
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
          if (resultKey == notNormalKey) documentStatus else DocumentStatus.Normal
        }
      }
    }
  }

  it should "show deleted cram records on queryAll, but not query" in {
    testQueryAll(DocumentStatus.Deleted)
  }

  it should "show External cram records on queryAll, but not query" in {
    testQueryAll(DocumentStatus.External)
  }

  def createMockFile(
    rootDir: File,
    baseName: String,
    extension: String
  ): (File, String) = {
    val fileContents = s"$randomId --- I am dummy '$extension' file --- $randomId"
    val filePath: File = rootDir / s"$baseName$extension"
    filePath.write(fileContents)
    (filePath, fileContents)
  }

  def testMoveCram(
    oldStyleCrai: Boolean = false,
    changeBasename: Boolean = false
  ): Future[Assertion] = {

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val flowcellBarcode = Random.alphanumeric.take(9).mkString
    val lane = Random.nextInt(7) + 1
    val libraryName = Random.alphanumeric.take(30).mkString
    val readgroup = s"$flowcellBarcode.$lane.$libraryName"
    val version = 3

    val cramName = s"$sample${CramExtensions.CramExtension}"
    val craiName =
      s"${if (oldStyleCrai) sample else cramName}"

    val rootSrc = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val (cramSrc, cramContents) =
      createMockFile(rootSrc, sample, CramExtensions.CramExtension)
    val (craiSrc, craiContents) =
      createMockFile(rootSrc, craiName, CramExtensions.CraiExtensionAddition)
    val (analysisFilesSrc, analysisFilesContents) =
      createMockFile(rootSrc, sample, CramExtensions.AnalysisFilesTxtExtension)
    val (alignmentSummaryMetricsSrc, alignmentSummaryMetricsContents) =
      createMockFile(rootSrc, sample, CramExtensions.AlignmentSummaryMetricsExtension)
    val (bamValidationReportSrc, bamValidationReportContents) =
      createMockFile(rootSrc, sample, CramExtensions.BamValidationReportExtension)
    val (cramValidationReportSrc, cramValidationReportContents) =
      createMockFile(rootSrc, sample, CramExtensions.CramValidationReportExtension)
    val (crosscheckSrc, crosscheckContents) =
      createMockFile(rootSrc, sample, CramExtensions.CrossCheckExtension)
    val (duplicateMetricsSrc, duplicateMetricsContents) =
      createMockFile(rootSrc, sample, CramExtensions.DuplicateMetricsExtension)
    val (fingerprintSrc, fingerprintContents) =
      createMockFile(rootSrc, sample, CramExtensions.FingerprintVcfExtension)
    val (fingerprintSummaryMetricsSrc, fingerprintingSummaryMetricsContents) =
      createMockFile(
        rootSrc,
        sample,
        CramExtensions.FingerprintingSummaryMetricsExtension
      )
    val (fingerprintDetailMetricsSrc, fingerprintingDetailMetricsContents) =
      createMockFile(
        rootSrc,
        sample,
        CramExtensions.FingerprintingDetailMetricsExtension
      )
    val (preAdapterSummaryMetricsSrc, preAdapterSummaryMetricsContents) =
      createMockFile(rootSrc, sample, CramExtensions.PreAdapterSummaryMetricsExtension)
    val (preAdapterDetailMetricsSrc, preAdapterDetailMetricsContents) =
      createMockFile(rootSrc, sample, CramExtensions.PreAdapterDetailMetricsExtension)
    val (preBqsrSelfSmSrc, preBqsrSelfSmContents) =
      createMockFile(rootSrc, sample, CramExtensions.PreBqsrSelfSMExtension)
    val (preBqsrDepthSmSrc, preBqsrDepthSmContents) =
      createMockFile(rootSrc, sample, CramExtensions.PreBqsrDepthSMExtension)
    val (hybridSelectionMetricsSrc, hybridSelectionMetricsContents) =
      createMockFile(rootSrc, sample, CramExtensions.HybridSelectionMetricsExtension)
    val (insertSizeMetricsSrc, insertSizeMetricsContents) =
      createMockFile(rootSrc, readgroup, ".insert_size_metrics")
    val (insertSizeHistogramSrc, insertSizeHistogramContents) =
      createMockFile(rootSrc, readgroup, ".insert_size_histogram.pdf")

    val endBasename = if (changeBasename) randomId else sample

    val rootDest = rootSrc.parent / s"moved/$randomId/"
    val cramDest = rootDest / s"$endBasename${CramExtensions.CramExtension}"
    val craiDest = rootDest / s"$endBasename${CramExtensions.CraiExtension}"
    val analysisFilesDest = rootDest / s"$endBasename${CramExtensions.AnalysisFilesTxtExtension}"
    val alignmentSummaryMetricsDest = rootDest / s"$endBasename${CramExtensions.AlignmentSummaryMetricsExtension}"
    val bamValidationReportDest = rootDest / s"$endBasename${CramExtensions.BamValidationReportExtension}"
    val cramValidationReportDest = rootDest / s"$endBasename${CramExtensions.CramValidationReportExtension}"
    val crosscheckDest = rootDest / s"$endBasename${CramExtensions.CrossCheckExtension}"
    val duplicateMetricsDest = rootDest / s"$endBasename${CramExtensions.DuplicateMetricsExtension}"
    val fingerprintDest = rootDest / s"$endBasename${CramExtensions.FingerprintVcfExtension}"
    val fingerprintSummaryMetricsDest = rootDest / s"$endBasename${CramExtensions.FingerprintingSummaryMetricsExtension}"
    val fingerprintDetailMetricsDest = rootDest / s"$endBasename${CramExtensions.FingerprintingDetailMetricsExtension}"
    val hybridSelectionMetricsDest = rootDest / s"$endBasename${CramExtensions.HybridSelectionMetricsExtension}"
    val preAdapterSummaryMetricsDest = rootDest / s"$endBasename${CramExtensions.PreAdapterSummaryMetricsExtension}"
    val preAdapterDetailMetricsDest = rootDest / s"$endBasename${CramExtensions.PreAdapterDetailMetricsExtension}"
    val preBqsrSelfSmDest = rootDest / s"$endBasename${CramExtensions.PreBqsrSelfSMExtension}"
    val preBqsrDepthSmDest = rootDest / s"$endBasename${CramExtensions.PreBqsrDepthSMExtension}"
    val insertSizeMetricsDest = rootDest / insertSizeMetricsSrc.name
    val insertSizeHistogramDest = rootDest / insertSizeHistogramSrc.name

    val key = CramKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSrc.uri),
      craiPath = Some(craiSrc.uri),
      analysisFilesTxtPath = Some(analysisFilesSrc.uri),
      alignmentSummaryMetricsPath = Some(alignmentSummaryMetricsSrc.uri),
      bamValidationReportPath = Some(bamValidationReportSrc.uri),
      cramValidationReportPath = Some(cramValidationReportSrc.uri),
      crosscheckPath = Some(crosscheckSrc.uri),
      duplicateMetricsPath = Some(duplicateMetricsSrc.uri),
      fingerprintPath = Some(fingerprintSrc.uri),
      fingerprintingSummaryMetricsPath = Some(fingerprintSummaryMetricsSrc.uri),
      fingerprintingDetailMetricsPath = Some(fingerprintDetailMetricsSrc.uri),
      hybridSelectionMetricsPath = Some(hybridSelectionMetricsSrc.uri),
      preAdapterSummaryMetricsPath = Some(preAdapterSummaryMetricsSrc.uri),
      preAdapterDetailMetricsPath = Some(preAdapterDetailMetricsSrc.uri),
      preBqsrSelfSmPath = Some(preBqsrSelfSmSrc.uri),
      preBqsrDepthSmPath = Some(preBqsrDepthSmSrc.uri),
      readgroupLevelMetricsFiles =
        Some(List(insertSizeMetricsSrc.uri, insertSizeHistogramSrc.uri))
    )

    val args = Seq.concat(
      Seq(
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--data-type",
        DataType.WGS.entryName,
        "--sample-alias",
        sample,
        "--version",
        version.toString,
        "--destination",
        rootDest.uri.toString
      ),
      if (changeBasename) {
        Seq("--new-basename", endBasename)
      } else {
        Seq.empty
      }
    )

    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runIgnore(ClioCommand.moveCramName, args: _*)
    } yield {
      Seq(
        cramSrc,
        craiSrc,
        analysisFilesSrc,
        alignmentSummaryMetricsSrc,
        bamValidationReportSrc,
        cramValidationReportSrc,
        crosscheckSrc,
        duplicateMetricsSrc,
        fingerprintSrc,
        fingerprintSummaryMetricsSrc,
        fingerprintDetailMetricsSrc,
        hybridSelectionMetricsSrc,
        preAdapterSummaryMetricsSrc,
        preAdapterDetailMetricsSrc,
        preBqsrSelfSmSrc,
        preBqsrDepthSmSrc,
        insertSizeMetricsSrc,
        insertSizeHistogramSrc
      ).foreach(_ shouldNot exist)

      Seq(
        cramDest,
        craiDest,
        analysisFilesDest,
        alignmentSummaryMetricsDest,
        bamValidationReportDest,
        cramValidationReportDest,
        crosscheckDest,
        duplicateMetricsDest,
        fingerprintDest,
        fingerprintSummaryMetricsDest,
        fingerprintDetailMetricsDest,
        hybridSelectionMetricsDest,
        preAdapterSummaryMetricsDest,
        preAdapterDetailMetricsDest,
        preBqsrSelfSmDest,
        preBqsrDepthSmDest,
        insertSizeMetricsDest,
        insertSizeHistogramDest
      ).foreach(_ should exist)

      Seq(
        (cramDest, cramContents),
        (craiDest, craiContents),
        (analysisFilesDest, analysisFilesContents),
        (alignmentSummaryMetricsDest, alignmentSummaryMetricsContents),
        (bamValidationReportDest, bamValidationReportContents),
        (cramValidationReportDest, cramValidationReportContents),
        (crosscheckDest, crosscheckContents),
        (duplicateMetricsDest, duplicateMetricsContents),
        (fingerprintDest, fingerprintContents),
        (fingerprintSummaryMetricsDest, fingerprintingSummaryMetricsContents),
        (fingerprintDetailMetricsDest, fingerprintingDetailMetricsContents),
        (hybridSelectionMetricsDest, hybridSelectionMetricsContents),
        (preAdapterSummaryMetricsDest, preAdapterSummaryMetricsContents),
        (preAdapterDetailMetricsDest, preAdapterDetailMetricsContents),
        (preBqsrSelfSmDest, preBqsrSelfSmContents),
        (preBqsrDepthSmDest, preBqsrDepthSmContents),
        (insertSizeMetricsDest, insertSizeMetricsContents),
        (insertSizeHistogramDest, insertSizeHistogramContents)
      ).foreach {
        case (dest, contents) =>
          dest.contentAsString should be(contents)
      }
      succeed
    }

    result.andThen {
      case _ => {
        val _ = Seq(
          cramSrc,
          cramDest,
          craiSrc,
          craiDest,
          analysisFilesSrc,
          analysisFilesDest,
          alignmentSummaryMetricsSrc,
          alignmentSummaryMetricsDest,
          bamValidationReportSrc,
          bamValidationReportDest,
          cramValidationReportSrc,
          cramValidationReportDest,
          crosscheckSrc,
          crosscheckDest,
          duplicateMetricsSrc,
          duplicateMetricsDest,
          fingerprintSrc,
          fingerprintDest,
          fingerprintSummaryMetricsSrc,
          fingerprintSummaryMetricsDest,
          fingerprintDetailMetricsSrc,
          fingerprintDetailMetricsDest,
          hybridSelectionMetricsSrc,
          hybridSelectionMetricsDest,
          preAdapterSummaryMetricsSrc,
          preAdapterSummaryMetricsDest,
          preAdapterDetailMetricsSrc,
          preAdapterDetailMetricsDest,
          preBqsrSelfSmSrc,
          preBqsrSelfSmDest,
          preBqsrDepthSmSrc,
          preBqsrDepthSmDest,
          insertSizeMetricsSrc,
          insertSizeMetricsDest,
          insertSizeHistogramSrc,
          insertSizeHistogramDest
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

  it should "not move crams without a destination" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.moveCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        randomId,
        "--data-type",
        DataType.WGS.entryName,
        "--sample-alias",
        randomId,
        "--version",
        "123"
      )
    }.map {
      _.getMessage should include("--destination")
    }
  }

  it should "not move crams with no registered files" in {
    val key = CramKey(
      Location.GCP,
      s"project$randomId",
      DataType.WGS,
      s"sample$randomId",
      1
    )
    runUpsertCram(key, CramMetadata()).flatMap { _ =>
      recoverToExceptionIf[Exception] {
        runDecode[UpsertId](
          ClioCommand.moveCramName,
          "--location",
          key.location.entryName,
          "--project",
          key.project,
          "--data-type",
          DataType.WGS.entryName,
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

  def testExternalCram(
    existingNote: Option[String] = None
  ): Future[Assertion] = {
    val markExternalNote =
      s"$randomId --- Marked External by the integration tests --- $randomId"

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cramContents = s"$randomId --- I am a cram fated for other worlds --- $randomId"
    val craiContents = s"$randomId --- I am an index fated for other worlds --- $randomId"
    val metrics1Contents = s"$randomId --- I am a questing metrics file --- $randomId"
    val metrics2Contents =
      s"$randomId --- I am a second questing metrics file --- $randomId"

    val storageDir = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramPath = storageDir / s"$randomId${CramExtensions.CramExtension}"
    val craiPath = storageDir / s"$randomId${CramExtensions.CraiExtensionAddition}"
    val metrics1Path = storageDir / s"$randomId.metrics"
    val metrics2Path = storageDir / s"$randomId.metrics"

    val key = CramKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramPath.uri),
      craiPath = Some(craiPath.uri),
      alignmentSummaryMetricsPath = Some(metrics1Path.uri),
      fingerprintingSummaryMetricsPath = Some(metrics1Path.uri),
      notes = existingNote
    )

    Seq(
      (cramPath, cramContents),
      (craiPath, craiContents),
      (metrics1Path, metrics1Contents),
      (metrics2Path, metrics2Contents)
    ).map {
      case (path, contents) => path.write(contents)
    }

    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runIgnore(
        ClioCommand.markExternalCramName,
        Seq(
          "--location",
          Location.GCP.entryName,
          "--project",
          project,
          "--data-type",
          DataType.WGS.entryName,
          "--sample-alias",
          sample,
          "--version",
          version.toString,
          "--note",
          markExternalNote
        ).filter(_.nonEmpty): _*
      )
      outputs <- runCollectJson(
        ClioCommand.queryCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--data-type",
        DataType.WGS.entryName,
        "--sample-alias",
        sample,
        "--version",
        version.toString,
        "--include-deleted"
      )
    } yield {

      outputs should have length 1
      val output = outputs.head
      output.unsafeGet[String]("notes") should be(
        metadata.notes.fold(markExternalNote)(existing => s"$existing\n$markExternalNote")
      )
      output.unsafeGet[DocumentStatus]("document_status") should be(
        DocumentStatus.External
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

  it should "mark crams as External when marking external" in {
    testExternalCram()
  }

  it should "require a note when marking a cram as External" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.markExternalCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        randomId,
        "--data-type",
        DataType.WGS.entryName,
        "--sample-alias",
        randomId,
        "--version",
        "123"
      )
    }.map {
      _.getMessage should include("--note")
    }
  }

  it should "preserve existing notes when marking crams as External" in testExternalCram(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

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

    val key = CramKey(Location.GCP, project, DataType.WGS, sample, version)
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
        ClioCommand.deleteCramName,
        Seq(
          "--location",
          Location.GCP.entryName,
          "--project",
          project,
          "--data-type",
          DataType.WGS.entryName,
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
        ClioCommand.queryCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--data-type",
        DataType.WGS.entryName,
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

  it should "preserve existing notes when deleting crams" in testDeleteCram(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  it should "not delete crams without a note" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.deleteCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        randomId,
        "--data-type",
        DataType.WGS.entryName,
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

  def testDeliverMetrics(
    deliverMetrics: Boolean,
    regulatoryDesignation: RegulatoryDesignation,
    customBillingProject: Boolean = false
  ): Future[Assertion] = {
    val id = randomId
    val project = s"project$id"
    val sample = s"sample$id"
    val version = 3

    val cramContents = s"$id --- I am a dummy cram --- $id"
    val craiContents = s"$id --- I am a dummy crai --- $id"
    val md5Contents = randomId
    val crosscheckContents = s"$id --- I am a dummy crosscheck --- $id"

    val cramName = s"$sample${CramExtensions.CramExtension}"
    val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${CramExtensions.Md5ExtensionAddition}"
    val crosscheckName = s"$cramName.crosscheck"

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName
    val crosscheckSource = rootSource / crosscheckName

    val prefix = "new_basename_"
    val newBasename = s"$prefix$sample"
    val rootDestination = rootSource.parent / s"moved/$id/"
    val cramDestination = rootDestination / s"$prefix$cramName"
    val craiDestination = rootDestination / s"$prefix$craiName"
    val md5Destination = rootDestination / s"$prefix$md5Name"
    val crosscheckDestination = rootDestination / s"$prefix$crosscheckName"

    val key = CramKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents)),
      documentStatus = Some(DocumentStatus.Normal),
      crosscheckPath = Some(crosscheckSource.uri),
      regulatoryDesignation = Some(regulatoryDesignation)
    )

    val workspaceName = s"$id-TestWorkspace-$id"
    val billingProject =
      if (customBillingProject) s"$id-TestBillingProject-$id"
      else ClioCommand.defaultBillingProject

    val _ = Seq(
      (cramSource, cramContents),
      (craiSource, craiContents),
      (crosscheckSource, crosscheckContents)
    ).map {
      case (source, contents) => source.write(contents)
    }

    val customBillingProjectArgs =
      if (customBillingProject) {
        Seq("--billing-project", billingProject)
      } else {
        Seq.empty
      }

    val commandArgs = Seq(
      "--location",
      Location.GCP.entryName,
      "--project",
      project,
      "--data-type",
      DataType.WGS.entryName,
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
    ) ++ (if (deliverMetrics) Some("--deliver-sample-metrics") else None) ++ customBillingProjectArgs
    val result = for {
      _ <- runUpsertCram(key, metadata)
      _ <- runIgnore(ClioCommand.deliverCramName, commandArgs: _*)
      outputs <- runCollectJson(
        ClioCommand.queryCramName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      (Seq(cramSource, craiSource) ++ (if (deliverMetrics)
                                         Some(crosscheckSource)
                                       else Some(crosscheckDestination)))
        .foreach(_ shouldNot exist)

      (Seq(cramDestination, craiDestination, md5Destination) ++ (if (deliverMetrics)
                                                                   Some(
                                                                     crosscheckDestination
                                                                   )
                                                                 else
                                                                   Some(
                                                                     crosscheckSource
                                                                   )))
        .foreach(_ should exist)

      Seq(
        (cramDestination, cramContents),
        (craiDestination, craiContents),
        (md5Destination, md5Contents),
        (
          if (deliverMetrics) crosscheckDestination else crosscheckSource,
          crosscheckContents
        )
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
      }

      outputs should contain only expectedMerge(
        key,
        metadata.copy(
          workspaceName = Some(workspaceName),
          billingProject = Some(billingProject),
          cramPath = Some(cramDestination.uri),
          craiPath = Some(craiDestination.uri),
          crosscheckPath = Some(
            (if (deliverMetrics) crosscheckDestination else crosscheckSource).uri
          )
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
          md5Destination,
          crosscheckSource,
          crosscheckDestination
        ).map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  RegulatoryDesignation.values.foreach(
    designation ⇒
      if (designation.isClinical) {
        it should s"throw an exception when delivering metrics and designation is $designation" in {
          recoverToSucceededIf[Exception] {
            testDeliverMetrics(true, designation)
          }
        }
      } else {
        it should s"move files, generate an md5 file, " +
          s"deliver metrics, and record the workspace name when delivering crams " +
          s"($designation, deliverMetrics=true)" in {
          testDeliverMetrics(true, designation)
        }
    }
  )
  RegulatoryDesignation.values.foreach { designation ⇒
    it should s"move files, generate an md5 file, not " +
      s"deliver metrics, and record the workspace name when delivering crams " +
      s"($designation, deliverMetrics=false)" in {
      testDeliverMetrics(false, designation)
    }
  }

  it should "deliver with a custom billing project" in {
    testDeliverMetrics(true, RegulatoryDesignation.ResearchOnly, true)
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

    val key = CramKey(Location.GCP, project, DataType.WGS, sample, version)
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
        ClioCommand.deliverCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--data-type",
        DataType.WGS.entryName,
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
        ClioCommand.queryCramName,
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
        key,
        metadata.copy(
          workspaceName = Some(workspaceName),
          billingProject = Some(ClioCommand.defaultBillingProject)
        )
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

    val key = CramKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = CramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents)),
      regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
    )

    def query = {
      for {
        results <- runCollectJson(
          ClioCommand.queryCramName,
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

    val key = CramKey(Location.GCP, project, DataType.WGS, sample, version)
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
        ClioCommand.deliverCramName,
        "--location",
        Location.GCP.entryName,
        "--project",
        project,
        "--data-type",
        DataType.WGS.entryName,
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
        ClioCommand.queryCramName,
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
        key,
        metadata.copy(
          workspaceName = Some(workspaceName),
          billingProject = Some(ClioCommand.defaultBillingProject),
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

    val key = CramKey(Location.GCP, project, DataType.WGS, sample, version)
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
          ClioCommand.deliverCramName,
          "--location",
          Location.GCP.entryName,
          "--project",
          project,
          "--data-type",
          DataType.WGS.entryName,
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
          ClioCommand.queryCramName,
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
