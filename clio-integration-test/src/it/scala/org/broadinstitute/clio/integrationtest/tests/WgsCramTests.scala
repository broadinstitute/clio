package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import com.sksamuel.elastic4s.IndexAndType
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram._
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation,
  UpsertId
}
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests of Clio's wgs-cram functionality. */
trait WgsCramTests extends ModelAutoDerivation { self: BaseIntegrationSpec =>

  def runUpsertCram(
    key: WgsCramKey,
    metadata: WgsCramMetadata,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runClient(
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
    ).mapTo[UpsertId]
  }

  it should "create the expected wgs-cram mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

    val expected = ElasticsearchIndex.WgsCram
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
    val expected = WgsCramQueryOutput(
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
      WgsCramKey(
        location,
        expected.project,
        expected.sampleAlias,
        expected.version
      ),
      WgsCramMetadata(cramPath = expected.cramPath)
    )

    it should s"handle upserts and queries for wgs-cram location $location" in {
      for {
        returnedUpsertId <- responseFuture
        outputs <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
          ClioCommand.queryWgsCramName,
          "--sample-alias",
          expected.sampleAlias
        )
      } yield {
        outputs should be(Seq(expected))

        val storedDocument = getJsonFrom(returnedUpsertId)(ElasticsearchIndex.WgsCram)
        ElasticsearchIndex.getByName[Location](storedDocument, "location") should be(
          expected.location
        )
        ElasticsearchIndex.getByName[String](storedDocument, "project") should be(
          expected.project
        )
        ElasticsearchIndex.getByName[String](storedDocument, "sample_alias") should be(
          expected.sampleAlias
        )
        ElasticsearchIndex.getByName[Int](storedDocument, "version") should be(
          expected.version
        )
        ElasticsearchIndex.getByName[URI](storedDocument, "cram_path") should be(
          expected.cramPath.get
        )
      }
    }
  }

  it should "assign different upsertIds to different wgs-cram upserts" in {
    val upsertKey = WgsCramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )

    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        WgsCramMetadata(
          cramPath = Some(
            URI.create(s"gs://path/cram1${WgsCramExtensions.CramExtension}")
          )
        )
      )
      upsertId2 <- runUpsertCram(
        upsertKey,
        WgsCramMetadata(
          cramPath = Some(
            URI.create(s"gs://path/cram2${WgsCramExtensions.CramExtension}")
          )
        )
      )
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsCram)
      ElasticsearchIndex.getByName[URI](storedDocument1, "cram_path") should be(
        URI.create(s"gs://path/cram1${WgsCramExtensions.CramExtension}")
      )

      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.WgsCram)
      ElasticsearchIndex.getByName[URI](storedDocument2, "cram_path") should be(
        URI.create(s"gs://path/cram2${WgsCramExtensions.CramExtension}")
      )

      storedDocument1
        .deepMerge(
          Map(
            ElasticsearchIndex.UpsertIdElasticsearchName -> upsertId2
          ).asJson
        )
        .deepMerge(
          Map(
            "cram_path" -> Some(
              URI.create(s"gs://path/cram2${WgsCramExtensions.CramExtension}")
            )
          ).asJson
        )
        .asObject
        .get should be(storedDocument2.asObject.get)
    }
  }

  it should "assign different upsertIds to equal wgs-cram upserts" in {
    val upsertKey = WgsCramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )
    val upsertData = WgsCramMetadata(
      cramPath = Some(URI.create(s"gs://path/cram1${WgsCramExtensions.CramExtension}"))
    )

    for {
      upsertId1 <- runUpsertCram(upsertKey, upsertData)
      upsertId2 <- runUpsertCram(upsertKey, upsertData)
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsCram)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.WgsCram)
      storedDocument1
        .deepMerge(
          Map(
            ElasticsearchIndex.UpsertIdElasticsearchName -> upsertId2
          ).asJson
        )
        .asObject
        .get should be(storedDocument2.asObject.get)
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
          val data = WgsCramMetadata(
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
      projectResults <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
        ClioCommand.queryWgsCramName,
        "--project",
        project
      )
      sampleResults <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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
          val key = WgsCramKey(location, project, sample, version)
          val data = WgsCramMetadata(
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
      prefixResults <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
        ClioCommand.queryWgsCramName,
        "--sample-alias",
        prefix
      )
      suffixResults <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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
    val cramPath = URI.create(s"gs://path/cram${WgsCramExtensions.CramExtension}")
    val metadata = WgsCramMetadata(
      cramPath = Some(cramPath),
      cramSize = Some(1000L),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
          ClioCommand.queryWgsCramName,
          "--project",
          project
        )
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = WgsCramMetadata(
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
      val upsertKey = WgsCramKey(
        location = Location.GCP,
        project = project,
        sampleAlias = sampleAlias,
        version = version
      )
      val upsertMetadata = WgsCramMetadata(
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
        results <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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

      results <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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

        val resultKey = WgsCramKey(
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

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName
    val alignmentMetricsSource = rootSource / alignmentMetricsName
    val fingerprintMetricsSource = rootSource / fingerprintMetricsName

    val endBasename = if (changeBasename) randomId else sample

    val rootDestination = rootSource.parent / s"moved/$randomId/"
    val cramDestination = rootDestination / s"$endBasename${WgsCramExtensions.CramExtension}"
    val craiDestination = rootDestination / s"$endBasename${WgsCramExtensions.CraiExtension}"
    val alignmentMetricsDestination = rootDestination / alignmentMetricsName
    val fingerprintMetricsDestination = rootDestination / fingerprintMetricsName

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = WgsCramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      alignmentSummaryMetricsPath = Some(alignmentMetricsSource.uri),
      fingerprintingSummaryMetricsPath = Some(fingerprintMetricsSource.uri)
    )

    val _ = Seq(
      (cramSource, cramContents),
      (craiSource, craiContents),
      (alignmentMetricsSource, alignmentMetricsContents),
      (fingerprintMetricsSource, fingerprintMetricsContents)
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
      _ <- runClient(ClioCommand.moveWgsCramName, args: _*)
    } yield {
      Seq(alignmentMetricsDestination, cramSource, craiSource)
        .foreach(_ shouldNot exist)

      Seq(alignmentMetricsSource, cramDestination, craiDestination)
        .foreach(_ should exist)

      // We don't deliver fingerprinting metrics for now because they're based on unpublished research.
      fingerprintMetricsSource should exist
      fingerprintMetricsDestination shouldNot exist

      Seq(
        (cramDestination, cramContents),
        (craiDestination, craiContents),
        (alignmentMetricsSource, alignmentMetricsContents),
        (fingerprintMetricsSource, fingerprintMetricsContents)
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
          alignmentMetricsSource,
          alignmentMetricsDestination,
          fingerprintMetricsSource,
          fingerprintMetricsDestination
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
    val key = WgsCramKey(
      Location.GCP,
      s"project$randomId",
      s"sample$randomId",
      1
    )
    runUpsertCram(key, WgsCramMetadata()).flatMap { _ =>
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
    val cramPath = storageDir / s"$randomId${WgsCramExtensions.CramExtension}"
    val craiPath = storageDir / s"$randomId${WgsCramExtensions.CraiExtensionAddition}"
    val metrics1Path = storageDir / s"$randomId.metrics"
    val metrics2Path = storageDir / s"$randomId.metrics"

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = WgsCramMetadata(
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
      _ <- runClient(
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
      outputs <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName = s"$cramName${WgsCramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${WgsCramExtensions.Md5ExtensionAddition}"

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
    val metadata = WgsCramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents))
    )

    val workspaceName = s"$id-TestWorkspace-$id"

    val _ = Seq((cramSource, cramContents), (craiSource, craiContents)).map {
      case (source, contents) => source.write(contents)
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
        rootDestination.uri.toString,
        "--new-basename",
        newBasename
      )
      outputs <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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

      outputs should be {
        Seq(
          WgsCramQueryOutput(
            location = Location.GCP,
            project = project,
            sampleAlias = sample,
            version = version,
            workspaceName = Some(workspaceName),
            cramPath = Some(cramDestination.uri),
            craiPath = Some(craiDestination.uri),
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

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName = s"$cramName${WgsCramExtensions.CraiExtensionAddition}"
    val md5Name = s"$cramName${WgsCramExtensions.Md5ExtensionAddition}"

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val md5Destination = rootSource / md5Name

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = WgsCramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents))
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    val _ = Seq((cramSource, cramContents), (craiSource, craiContents)).map {
      case (source, contents) => source.write(contents)
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
        rootSource.uri.toString
      )
      outputs <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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

      outputs should be {
        Seq(
          WgsCramQueryOutput(
            location = Location.GCP,
            project = project,
            sampleAlias = sample,
            version = version,
            workspaceName = Some(workspaceName),
            cramPath = Some(cramSource.uri),
            craiPath = Some(craiSource.uri),
            cramMd5 = Some(Symbol(md5Contents)),
            documentStatus = Some(DocumentStatus.Normal)
          )
        )
      }
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

    val cramName = s"$sample${WgsCramExtensions.CramExtension}"
    val craiName = s"$cramName${WgsCramExtensions.CraiExtensionAddition}"

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = WgsCramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents)),
      regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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

    val cramRegulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val rootDestination = rootSource.parent / s"moved/$randomId/"
    val cramDestination = rootDestination / cramName
    val craiDestination = rootDestination / craiName
    val md5Destination = rootDestination / md5Name

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = WgsCramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
      cramMd5 = Some(Symbol(md5Contents)),
      regulatoryDesignation = cramRegulatoryDesignation
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    val _ = Seq((cramSource, cramContents), (craiSource, craiContents)).map {
      case (source, contents) => source.write(contents)
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
        rootDestination.uri.toString
      )
      outputs <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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

      outputs should be {
        Seq(
          WgsCramQueryOutput(
            location = Location.GCP,
            project = project,
            sampleAlias = sample,
            version = version,
            workspaceName = Some(workspaceName),
            cramPath = Some(cramDestination.uri),
            craiPath = Some(craiDestination.uri),
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
        ).map(_.delete(swallowIOExceptions = true))
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

    val rootSource = rootTestStorageDir / s"cram/$project/$sample/v$version/"
    val cramSource = rootSource / cramName
    val craiSource = rootSource / craiName

    val rootDestination = rootSource.parent / s"moved/$randomId/"

    val key = WgsCramKey(Location.GCP, project, sample, version)
    val metadata = WgsCramMetadata(
      cramPath = Some(cramSource.uri),
      craiPath = Some(craiSource.uri),
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
          rootDestination.uri.toString
        )
      } yield {
        deliverResponse
      }
    }.flatMap { _ =>
      for {
        outputs <- runClientGetJsonAs[Seq[WgsCramQueryOutput]](
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
    val upsertKey = WgsCramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )
    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        WgsCramMetadata(notes = Some("I'm a note")),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsCram)
      ElasticsearchIndex.getByName[String](storedDocument1, "notes") should be(
        "I'm a note"
      )
    }
  }

  it should "allow an upsert that modifies values not already set or are unchanged if force is false" in {
    val upsertKey = WgsCramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )
    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        WgsCramMetadata(notes = Some("I'm a note")),
        force = false
      )
      upsertId2 <- runUpsertCram(
        upsertKey,
        WgsCramMetadata(notes = Some("I'm a note"), cramSize = Some(12345)),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsCram)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.WgsCram)
      ElasticsearchIndex.getByName[String](storedDocument1, "notes") should be(
        "I'm a note"
      )
      ElasticsearchIndex.getByName[Int](storedDocument2, "cram_size") should be(12345)
    }
  }

  it should "not allow an upsert that modifies values already set if force is false" in {
    val upsertKey = WgsCramKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1
    )
    for {
      upsertId1 <- runUpsertCram(
        upsertKey,
        WgsCramMetadata(notes = Some("I'm a note")),
        force = false
      )
      _ <- recoverToSucceededIf[Exception] {
        runUpsertCram(
          upsertKey,
          WgsCramMetadata(
            notes = Some("I'm a different note")
          ),
          force = false
        )
      }
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsCram)
      ElasticsearchIndex.getByName[String](storedDocument1, "notes") should be(
        "I'm a note"
      )
    }
  }
}
