package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient.FailedResponse
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfExtensions, GvcfKey, GvcfMetadata}
import org.broadinstitute.clio.util.model._
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests of Clio's gvcf functionality. */
trait GvcfTests { self: BaseIntegrationSpec =>
  import org.broadinstitute.clio.JsonUtils.JsonOps

  def runUpsertGvcf(
    key: GvcfKey,
    metadata: GvcfMetadata,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runDecode[UpsertId](
      ClioCommand.addGvcfName,
      Seq(
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
        "--metadata-location",
        tmpMetadata.toString,
        if (force) "--force" else ""
      ).filter(_.nonEmpty): _*
    )
  }

  it should "create the expected gvcf mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

    val expected = ElasticsearchIndex.Gvcf
    val getRequest = getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
  }

  // Generate a test for every possible Location value.
  Location.values.foreach {
    it should behave like testGvcfLocation(_)
  }

  it should "handle raw queries for specific fields only" in {
    val sampleAlias = s"someAlias $randomId"
    val key = GvcfKey(
      location = Location.GCP,
      project = "test project",
      sampleAlias = sampleAlias,
      version = 2,
      dataType = DataType.WGS
    )
    val gvcfPath = Some(URI.create(s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"))
    val metadata = GvcfMetadata(
      gvcfPath = gvcfPath,
      documentStatus = Some(DocumentStatus.Normal)
    )
    val expected = Map("gvcf_path" -> gvcfPath).asJson
    val rawJsonQuery =
      s"""{
         |  "_source": "gvcf_path",
         |  "query": {
         |    "bool": {
         |      "must": [
         |        {
         |          "query_string": {
         |            "default_field": "sample_alias.exact",
         |            "query": "\\"$sampleAlias\\""
         |          }
         |        }
         |      ]
         |    }
         |  }
         |}
      """.stripMargin
    rawQueryTest(
      rawJsonQuery,
      runUpsertGvcf(key, metadata),
      _ should contain only expected
    )
  }

  it should "handle raw queries returning more than one document" in {
    val sampleAlias = s"someAlias $randomId"
    val key = GvcfKey(
      location = Location.GCP,
      project = "test project",
      sampleAlias = sampleAlias,
      version = 2,
      dataType = DataType.WGS
    )
    val gvcfPath = Some(URI.create(s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"))
    val metadata = GvcfMetadata(
      gvcfPath = gvcfPath,
      documentStatus = Some(DocumentStatus.Normal)
    )
    def setup(): Future[Unit] =
      for {
        _ <- runUpsertGvcf(key, metadata)
        _ <- runUpsertGvcf(
          key.copy(project = "a different test project"),
          metadata.copy(notes = Some("this is a different note"))
        )
      } yield ()
    val expected = Map("gvcf_path" -> gvcfPath).asJson
    val rawJsonQuery =
      s"""{
         |  "_source": "gvcf_path",
         |  "query": {
         |    "bool": {
         |      "must": [
         |        {
         |          "query_string": {
         |            "default_field": "sample_alias.exact",
         |            "query": "\\"$sampleAlias\\""
         |          }
         |        }
         |      ]
         |    }
         |  }
         |}
      """.stripMargin
    rawQueryTest(
      rawJsonQuery,
      setup(),
      _ should be(Seq(expected, expected))
    )
  }

  it should "fail when submitting a bogus raw query that is valid json" in {
    val rawJsonQuery =
      s"""{
         |  "valid": "json",
         |  "but": "bogus",
         |  "elasticsearch": "query"
         |}
      """.stripMargin
    recoverToSucceededIf[FailedResponse] {
      rawQueryTest(
        rawJsonQuery,
        Future.successful(()),
        _ => succeed
      )
    }
  }

  def rawQueryTest(
    input: String,
    setup: => Future[_],
    assertions: Seq[Json] => Assertion
  ): Future[Assertion] = {
    val tempFile = writeLocalTmpFile(input)
    for {
      _ <- setup
      outputs <- runCollectJson(
        ClioCommand.rawQueryGvcfName,
        "--query-input-path",
        tempFile.path.toString
      )
    } yield assertions(outputs)
  }

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testGvcfLocation(location: Location): Unit = {
    it should s"handle upserts and queries for gvcf location $location" in {
      val key = GvcfKey(
        location = location,
        project = "test project",
        sampleAlias = s"someAlias $randomId",
        version = 2,
        dataType = DataType.WGS
      )
      val metadata = GvcfMetadata(
        gvcfPath = Some(URI.create(s"gs://path/gvcf${GvcfExtensions.GvcfExtension}")),
        documentStatus = Some(DocumentStatus.Normal)
      )
      val expected = expectedMerge(key, metadata)

      for {
        returnedUpsertId <- runUpsertGvcf(key, metadata.copy(documentStatus = None))
        outputs <- runCollectJson(
          ClioCommand.queryGvcfName,
          "--sample-alias",
          key.sampleAlias
        )
      } yield {
        outputs should contain only expected
        val storedDocument = getJsonFrom(returnedUpsertId)(ElasticsearchIndex.Gvcf)
        storedDocument.mapObject(
          _.filterKeys(!ElasticsearchIndex.BookkeepingNames.contains(_))
        ) should be(expected)
      }
    }
  }

  it should "assign different upsertIds to different gvcf upserts" in {
    val upsertKey = GvcfKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )

    val gvcfUri1 = URI.create(s"gs://path/gvcf1${GvcfExtensions.GvcfExtension}")
    val gvcfUri2 = URI.create(s"gs://path/gvcf2${GvcfExtensions.GvcfExtension}")

    for {
      upsertId1 <- runUpsertGvcf(
        upsertKey,
        GvcfMetadata(gvcfPath = Some(gvcfUri1))
      )
      upsertId2 <- runUpsertGvcf(
        upsertKey,
        GvcfMetadata(gvcfPath = Some(gvcfUri2))
      )
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Gvcf)
      storedDocument1.unsafeGet[URI]("gvcf_path") should be(gvcfUri1)

      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Gvcf)
      storedDocument2.unsafeGet[URI]("gvcf_path") should be(gvcfUri2)

      storedDocument1.deepMerge {
        Json.obj(
          ElasticsearchIndex.UpsertIdElasticsearchName -> upsertId2.asJson,
          "gvcf_path" -> gvcfUri2.asJson
        )
      } should be(storedDocument2)
    }
  }

  it should "assign different upsertIds to equal gvcf upserts" in {
    val upsertKey = GvcfKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    val upsertData = GvcfMetadata(
      gvcfPath = Some(URI.create(s"gs://path/gvcf1${GvcfExtensions.GvcfExtension}"))
    )

    for {
      upsertId1 <- runUpsertGvcf(upsertKey, upsertData)
      upsertId2 <- runUpsertGvcf(upsertKey, upsertData)
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Gvcf)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Gvcf)
      storedDocument1.mapObject(
        _.add(ElasticsearchIndex.UpsertIdElasticsearchName, upsertId2.asJson)
      ) should be(storedDocument2)
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
          val key = GvcfKey(location, project, DataType.WGS, sample, version)
          val data = GvcfMetadata(
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
      projectResults <- runCollectJson(
        ClioCommand.queryGvcfName,
        "--project",
        project
      )
      sampleResults <- runCollectJson(
        ClioCommand.queryGvcfName,
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

  it should "handle updates to gvcf metadata" in {
    val project = s"testProject$randomId"
    val key = GvcfKey(Location.GCP, project, DataType.WGS, s"testSample$randomId", 1)
    val gvcfPath = URI.create(s"gs://path/gvcf${GvcfExtensions.GvcfExtension}")
    val metadata = GvcfMetadata(
      gvcfPath = Some(gvcfPath),
      contamination = Some(.75f),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        results <- runCollectJson(
          ClioCommand.queryGvcfName,
          "--project",
          project
        )
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = GvcfMetadata(
      contamination = metadata.contamination,
      gvcfPath = metadata.gvcfPath
    )

    for {
      _ <- runUpsertGvcf(key, upsertData)
      original <- query
      _ = original.unsafeGet[URI]("gvcf_path") should be(gvcfPath)
      _ = original.unsafeGet[Float]("contamination") should be(.75f)
      _ = original.unsafeGet[Option[String]]("notes") should be(None)

      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertGvcf(key, upsertData2)
      withNotes <- query
      _ = withNotes.unsafeGet[URI]("gvcf_path") should be(gvcfPath)
      _ = withNotes.unsafeGet[Float]("contamination") should be(.75f)
      _ = withNotes.unsafeGet[String]("notes") should be("Breaking news")

      _ <- runUpsertGvcf(
        key,
        upsertData2
          .copy(contamination = Some(0.123f), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.unsafeGet[URI]("gvcf_path") should be(gvcfPath)
      emptyNotes.unsafeGet[Float]("contamination") should be(.123f)
      emptyNotes.unsafeGet[String]("notes") should be("")
    }
  }

  it should "show deleted gvcf records on queryAll, but not query" in {
    val project = "testProject" + randomId
    val sampleAlias = "sample688." + randomId

    val keysWithMetadata = (1 to 3).map { version =>
      val upsertKey = GvcfKey(
        location = Location.GCP,
        project = project,
        sampleAlias = sampleAlias,
        version = version,
        dataType = DataType.WGS
      )
      val upsertMetadata = GvcfMetadata(
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
        results <- runCollectJson(
          ClioCommand.queryGvcfName,
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
      _ <- runUpsertGvcf(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runCollectJson(
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
        result.unsafeGet[String]("project") should be(project)
        result.unsafeGet[String]("sample_alias") should be(sampleAlias)

        val resultKey = GvcfKey(
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

  it should "respect user-set regulatory designation for gvcfs" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val cloudPath = rootTestStorageDir / s"gvcf/$project/$sample/v$version/$randomId${GvcfExtensions.GvcfExtension}"

    val key = GvcfKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = GvcfMetadata(
      gvcfPath = Some(cloudPath.uri),
      regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
    )

    def query = {
      for {
        results <- runCollectJson(
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
      result.unsafeGet[RegulatoryDesignation]("regulatory_designation") should be(
        RegulatoryDesignation.ClinicalDiagnostics
      )
    }
  }

  it should "preserve any existing regulatory designation for gvcfs" in {
    val key = GvcfKey(
      Location.GCP,
      s"project$randomId",
      dataType = DataType.WGS,
      s"sample$randomId",
      3
    )
    val firstMetadata = GvcfMetadata(
      regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
    )
    val secondMetadata = GvcfMetadata(
      gvcfPath = Some(URI.create(s"gs://gvcf/$randomId${GvcfExtensions.GvcfExtension}"))
    )
    val expectedOutput =
      expectedMerge(
        key,
        firstMetadata.copy(
          gvcfPath = secondMetadata.gvcfPath,
          documentStatus = Some(DocumentStatus.Normal)
        )
      )

    for {
      _ <- runUpsertGvcf(key, firstMetadata)
      _ <- runUpsertGvcf(key, secondMetadata)
      queryOutput <- runCollectJson(
        ClioCommand.queryGvcfName,
        "--project",
        key.project,
        "--sample-alias",
        key.sampleAlias,
        "--version",
        key.version.toString
      )
    } yield {
      queryOutput should contain only expectedOutput
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
    val summaryMetricsContents = s"$randomId --- I am dummy summary metrics --- $randomId"
    val detailMetricsContents = s"$randomId --- I am dummy detail metrics --- $randomId"

    val gvcfName = s"$randomId${GvcfExtensions.GvcfExtension}"
    val indexName = s"$randomId${GvcfExtensions.IndexExtension}"
    val summaryMetricsName = s"$randomId${GvcfExtensions.SummaryMetricsExtension}"
    val detailMetricsName = s"$randomId${GvcfExtensions.DetailMetricsExtension}"

    val rootSource = rootTestStorageDir / s"gvcf/$project/$sample/v$version/"
    val rootDestination = if (srcIsDest) {
      rootSource
    } else {
      rootSource.parent / s"moved/$randomId/"
    }

    val gvcfSource = if (gvcfInDest) {
      rootDestination / gvcfName
    } else {
      rootSource / gvcfName
    }
    val indexSource = rootSource / indexName
    val summaryMetricsSource = rootSource / summaryMetricsName
    val detailMetricsSource = rootSource / detailMetricsName

    val gvcfDestination = rootDestination / gvcfName
    val indexDestination = rootDestination / indexName
    val summaryMetricsDestination = rootDestination / summaryMetricsName
    val detailMetricsDestination = rootDestination / detailMetricsName

    val key = GvcfKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = GvcfMetadata(
      gvcfPath = Some(gvcfSource.uri),
      gvcfIndexPath = Some(indexSource.uri),
      gvcfSummaryMetricsPath = Some(summaryMetricsSource.uri),
      gvcfDetailMetricsPath = Some(detailMetricsSource.uri)
    )

    val _ = Seq(
      (gvcfSource, gvcfContents),
      (indexSource, indexContents),
      (summaryMetricsSource, summaryMetricsContents),
      (detailMetricsSource, detailMetricsContents)
    ).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertGvcf(key, metadata)
      _ <- runIgnore(
        ClioCommand.moveGvcfName,
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
        rootDestination.uri.toString
      )
    } yield {
      if (!(srcIsDest || gvcfInDest)) {
        gvcfSource shouldNot exist
      }
      if (!srcIsDest) {
        Seq(indexSource, summaryMetricsSource, detailMetricsSource)
          .foreach(_ shouldNot exist)
      }
      Seq(
        gvcfDestination,
        indexDestination,
        summaryMetricsDestination,
        detailMetricsDestination
      ).foreach(_ should exist)
      Seq(
        (gvcfDestination, gvcfContents),
        (indexDestination, indexContents),
        (summaryMetricsDestination, summaryMetricsContents),
        (detailMetricsDestination, detailMetricsContents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
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
        ).map(_.delete(swallowIOExceptions = true))
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
      runDecode[UpsertId](
        ClioCommand.moveGvcfName,
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

  def testDeleteGvcf(
    existingNote: Option[String] = None,
    testNonExistingFile: Boolean = false,
    force: Boolean = false
  ): Future[Assertion] = {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val gvcfContents = s"$randomId --- I am a gvcf fated to die --- $randomId"
    val indexContents = s"$randomId --- I am an index fated to die --- $randomId"
    val summaryMetricsContents =
      s"$randomId --- I am an immortal summary metrics file --- $randomId"
    val detailMetricsContents =
      s"$randomId --- I am an immortal detail metrics file --- $randomId"

    val deleteNote = s"$randomId --- Deleted by the integration tests --- $randomId"

    val storageDir = rootTestStorageDir / s"gvcf/$project/$sample/v$version/"
    val gvcfPath = storageDir / s"$randomId${GvcfExtensions.GvcfExtension}"
    val indexPath = storageDir / s"$randomId${GvcfExtensions.IndexExtension}"
    val summaryMetricsPath = storageDir / s"$randomId${GvcfExtensions.SummaryMetricsExtension}"
    val detailMetricsPath = storageDir / s"$randomId${GvcfExtensions.DetailMetricsExtension}"

    val key = GvcfKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = GvcfMetadata(
      gvcfPath = Some(gvcfPath.uri),
      gvcfIndexPath = Some(indexPath.uri),
      gvcfSummaryMetricsPath = Some(summaryMetricsPath.uri),
      gvcfDetailMetricsPath = Some(detailMetricsPath.uri),
      notes = existingNote
    )

    val _ = if (!testNonExistingFile) {
      Seq(
        (gvcfPath, gvcfContents),
        (indexPath, indexContents),
        (summaryMetricsPath, summaryMetricsContents),
        (detailMetricsPath, detailMetricsContents)
      ).map {
        case (path, contents) => path.write(contents)
      }
    }

    val result = for {
      _ <- runUpsertGvcf(key, metadata)
      _ <- runIgnore(
        ClioCommand.deleteGvcfName,
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
        ClioCommand.queryGvcfName,
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
      gvcfPath shouldNot exist
      indexPath shouldNot exist
      if (!testNonExistingFile) {
        summaryMetricsPath should exist
        detailMetricsPath should exist
        summaryMetricsPath.contentAsString should be(summaryMetricsContents)
        detailMetricsPath.contentAsString should be(detailMetricsContents)
      }

      outputs should have length 1
      val output = outputs.head
      output.unsafeGet[String]("notes") should be {
        existingNote.fold(deleteNote)(existing => s"$existing\n$deleteNote")
      }
      output.unsafeGet[DocumentStatus]("document_status") should be(
        DocumentStatus.Deleted
      )
    }

    result.andThen[Unit] {
      case _ => {
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Seq(gvcfPath, indexPath, summaryMetricsPath, detailMetricsPath)
          .map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "delete gvcfs and their indexes in GCP, but not their metrics" in testDeleteGvcf()

  it should "preserve existing notes when deleting gvcfs" in testDeleteGvcf(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )
  it should "throw an exception when trying to delete a gvcf if a file does not exist" in {
    recoverToSucceededIf[Exception] {
      testDeleteGvcf(testNonExistingFile = true)
    }
  }

  it should "delete a gvcf if a file does not exist and force is true" in testDeleteGvcf(
    testNonExistingFile = true,
    force = true
  )

  it should "not delete gvcfs without a note" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.deleteGvcfName,
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

  it should "upsert a new gvcf if force is false" in {
    val upsertKey = GvcfKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertGvcf(
        upsertKey,
        GvcfMetadata(notes = Some("I'm a note")),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Gvcf)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
    }
  }

  it should "allow an upsert that modifies values not already set or are unchanged if force is false" in {
    val upsertKey = GvcfKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertGvcf(
        upsertKey,
        GvcfMetadata(notes = Some("I'm a note")),
        force = false
      )
      upsertId2 <- runUpsertGvcf(
        upsertKey,
        GvcfMetadata(notes = Some("I'm a note"), gvcfSize = Some(12345)),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Gvcf)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Gvcf)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
      storedDocument2.unsafeGet[Int]("gvcf_size") should be(12345)
    }
  }

  it should "not allow an upsert that modifies values already set if force is false" in {
    val upsertKey = GvcfKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertGvcf(
        upsertKey,
        GvcfMetadata(notes = Some("I'm a note")),
        force = false
      )
      _ <- recoverToSucceededIf[Exception] {
        runUpsertGvcf(
          upsertKey,
          GvcfMetadata(notes = Some("I'm a different note")),
          force = false
        )
      }
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Gvcf)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
    }
  }
}
