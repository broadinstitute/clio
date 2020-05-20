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
import org.broadinstitute.clio.transfer.model.bam._
import org.broadinstitute.clio.util.model._
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests of Clio's bam functionality. */
trait BamTests { self: BaseIntegrationSpec =>
  import org.broadinstitute.clio.JsonUtils.JsonOps

  def runUpsertBam(
    key: BamKey,
    metadata: BamMetadata,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runDecode[UpsertId](
      ClioCommand.addBamName,
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

  it should "create the expected bam mapping in elasticsearch" in {
    import ElasticsearchUtil.HttpClientOps
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val expected = ElasticsearchIndex.Bam
    val getRequest = getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
  }

  // Generate a test for every possible Location value.
  Location.values.foreach {
    it should behave like testBamLocation(_)
  }

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testBamLocation(location: Location): Unit = {
    it should s"handle upserts and queries for bam location $location" in {
      val key = BamKey(
        location = location,
        project = "test project",
        sampleAlias = s"someAlias $randomId",
        version = 2,
        dataType = DataType.WGS
      )
      val metadata = BamMetadata(
        documentStatus = Some(DocumentStatus.Normal),
        bamPath = Some(URI.create(s"gs://path/bam${BamExtensions.BamExtension}"))
      )
      val expected = expectedMerge(key, metadata)

      for {
        returnedUpsertId <- runUpsertBam(key, metadata.copy(documentStatus = None))
        outputs <- runCollectJson(
          ClioCommand.queryBamName,
          "--sample-alias",
          key.sampleAlias
        )
      } yield {
        outputs should contain only expected
        val storedDocument = getJsonFrom(returnedUpsertId)(ElasticsearchIndex.Bam)
        storedDocument.mapObject(
          _.filterKeys(!ElasticsearchIndex.BookkeepingNames.contains(_))
        ) should be(expected)
      }
    }
  }

  it should "assign different upsertIds to different bam upserts" in {
    val upsertKey = BamKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )

    for {
      upsertId1 <- runUpsertBam(
        upsertKey,
        BamMetadata(
          bamPath = Some(
            URI.create(s"gs://path/bam1${BamExtensions.BamExtension}")
          )
        )
      )
      upsertId2 <- runUpsertBam(
        upsertKey,
        BamMetadata(
          bamPath = Some(
            URI.create(s"gs://path/bam2${BamExtensions.BamExtension}")
          )
        )
      )
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Bam)
      storedDocument1.unsafeGet[URI]("bam_path") should be(
        URI.create(s"gs://path/bam1${BamExtensions.BamExtension}")
      )

      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Bam)
      storedDocument2.unsafeGet[URI]("bam_path") should be(
        URI.create(s"gs://path/bam2${BamExtensions.BamExtension}")
      )

      storedDocument1.deepMerge {
        Json.obj(
          ElasticsearchIndex.UpsertIdElasticsearchName -> upsertId2.asJson,
          "bam_path" -> s"gs://path/bam2${BamExtensions.BamExtension}".asJson
        )
      } should be(storedDocument2)
    }
  }

  it should "assign different upsertIds to equal bam upserts" in {
    val upsertKey = BamKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    val upsertData = BamMetadata(
      bamPath = Some(URI.create(s"gs://path/bam1${BamExtensions.BamExtension}"))
    )

    for {
      upsertId1 <- runUpsertBam(upsertKey, upsertData)
      upsertId2 <- runUpsertBam(upsertKey, upsertData)
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Bam)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Bam)
      storedDocument1.mapObject(
        _.add(ElasticsearchIndex.UpsertIdElasticsearchName, upsertId2.asJson)
      ) should be(storedDocument2)
    }
  }

  it should "handle querying bams by sample and project" in {
    val location = Location.GCP
    val project = "testProject" + randomId

    val samples = {
      val sameId = "testSample" + randomId
      Seq(sameId, sameId, "testSample" + randomId)
    }

    val upserts = Future.sequence {
      samples.zip(1 to 3).map {
        case (sample, version) =>
          val key = BamKey(location, project, DataType.WGS, sample, version)
          val data = BamMetadata(
            bamPath = Some(
              URI.create(s"gs://path/bam${BamExtensions.BamExtension}")
            ),
            bamSize = Some(1000L)
          )
          runUpsertBam(key, data)
      }
    }

    for {
      _ <- upserts
      projectResults <- runCollectJson(
        ClioCommand.queryBamName,
        "--project",
        project
      )
      sampleResults <- runCollectJson(
        ClioCommand.queryBamName,
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
          val key = BamKey(location, project, DataType.WGS, sample, version)
          val data = BamMetadata(
            bamPath = Some(
              URI.create(s"gs://path/bam${BamExtensions.BamExtension}")
            ),
            bamSize = Some(1000L)
          )
          runUpsertBam(key, data)
      }
    }

    for {
      _ <- upserts
      prefixResults <- runCollectJson(
        ClioCommand.queryBamName,
        "--sample-alias",
        prefix
      )
      suffixResults <- runCollectJson(
        ClioCommand.queryBamName,
        "--sample-alias",
        suffix
      )
    } yield {
      prefixResults should have length 1
      suffixResults should have length 1
    }
  }

  it should "handle updates to bam metadata" in {
    val project = s"testProject$randomId"
    val key = BamKey(Location.GCP, project, DataType.WGS, s"testSample$randomId", 1)
    val bamPath = URI.create(s"gs://path/bam${BamExtensions.BamExtension}")
    val bamSize = 1000L
    val initialNotes = "Breaking news"
    val metadata = BamMetadata(
      bamPath = Some(bamPath),
      bamSize = Some(bamSize),
      notes = Some(initialNotes)
    )

    def query = {
      for {
        results <- runCollectJson(
          ClioCommand.queryBamName,
          "--project",
          project
        )
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = BamMetadata(
      bamSize = metadata.bamSize,
      bamPath = metadata.bamPath
    )

    for {
      _ <- runUpsertBam(key, upsertData)
      original <- query
      _ = original.unsafeGet[URI]("bam_path") should be(bamPath)
      _ = original.unsafeGet[Long]("bam_size") should be(bamSize)
      _ = original.unsafeGet[Option[String]]("notes") should be(None)

      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertBam(key, upsertData2)
      withNotes <- query
      _ = withNotes.unsafeGet[URI]("bam_path") should be(bamPath)
      _ = withNotes.unsafeGet[Long]("bam_size") should be(bamSize)
      _ = withNotes.unsafeGet[String]("notes") should be(initialNotes)

      _ <- runUpsertBam(
        key,
        upsertData2.copy(bamSize = Some(2000L), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.unsafeGet[URI]("bam_path") should be(bamPath)
      emptyNotes.unsafeGet[Long]("bam_size") should be(2000L)
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
      val upsertKey = BamKey(
        location = Location.GCP,
        project = project,
        sampleAlias = sampleAlias,
        version = version,
        dataType = DataType.WGS
      )
      val upsertMetadata = BamMetadata(
        bamPath = Some(URI.create(s"gs://bam/$sampleAlias.$version"))
      )
      (upsertKey, upsertMetadata)
    }
    val (notNormalKey, notNormalData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (v1Key, metadata) => runUpsertBam(v1Key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        results <- runCollectJson(
          ClioCommand.queryBamName,
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
      _ <- runUpsertBam(
        notNormalKey,
        notNormalData.copy(documentStatus = Some(documentStatus))
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runCollectJson(
        ClioCommand.queryBamName,
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

        val resultKey = BamKey(
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

  it should "show deleted bam records on queryAll, but not query" in {
    testQueryAll(DocumentStatus.Deleted)
  }

  it should "show External bam records on queryAll, but not query" in {
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

  def testMoveBam(
    oldStyleBai: Boolean = false,
    changeBasename: Boolean = false
  ): Future[Assertion] = {

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val bamName = s"$sample${BamExtensions.BamExtension}"
    val baiName =
      s"${if (oldStyleBai) sample else bamName}"

    val rootSrc = rootTestStorageDir / s"bam/$project/$sample/v$version/"
    val (bamSrc, bamContents) =
      createMockFile(rootSrc, sample, BamExtensions.BamExtension)
    val (baiSrc, baiContents) =
      createMockFile(rootSrc, baiName, BamExtensions.BaiExtensionAddition)

    val endBasename = if (changeBasename) randomId else sample

    val rootDest = rootSrc.parent / s"moved/$randomId/"
    val bamDest = rootDest / s"$endBasename${BamExtensions.BamExtension}"
    val baiDest = rootDest / s"$endBasename${BamExtensions.BaiExtension}"

    val key = BamKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = BamMetadata(
      bamPath = Some(bamSrc.uri),
      baiPath = Some(baiSrc.uri)
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
      _ <- runUpsertBam(key, metadata)
      _ <- runIgnore(ClioCommand.moveBamName, args: _*)
    } yield {
      Seq(
        bamSrc,
        baiSrc
      ).foreach(_ shouldNot exist)

      Seq(
        bamDest,
        baiDest
      ).foreach(_ should exist)

      Seq(
        (bamDest, bamContents),
        (baiDest, baiContents)
      ).foreach {
        case (dest, contents) =>
          dest.contentAsString should be(contents)
      }
      succeed
    }

    result.andThen {
      case _ => {
        val _ = Seq(
          bamSrc,
          bamDest,
          baiSrc,
          baiDest
        ).map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "move the bam and bai together in GCP" in testMoveBam()

  it should "fixup the bai extension on move" in testMoveBam(
    oldStyleBai = true
  )

  it should "support changing the bam and bai basename on move" in testMoveBam(
    changeBasename = true
  )

  it should "not move bams without a destination" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.moveBamName,
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

  it should "not move bams with no registered files" in {
    val key = BamKey(
      Location.GCP,
      s"project$randomId",
      DataType.WGS,
      s"sample$randomId",
      1
    )
    runUpsertBam(key, BamMetadata()).flatMap { _ =>
      recoverToExceptionIf[Exception] {
        runDecode[UpsertId](
          ClioCommand.moveBamName,
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

  def testExternalBam(
    existingNote: Option[String] = None
  ): Future[Assertion] = {
    val markExternalNote =
      s"$randomId --- Marked External by the integration tests --- $randomId"

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val bamContents = s"$randomId --- I am a bam fated for other worlds --- $randomId"
    val baiContents = s"$randomId --- I am an index fated for other worlds --- $randomId"

    val storageDir = rootTestStorageDir / s"bam/$project/$sample/v$version/"
    val bamPath = storageDir / s"$randomId${BamExtensions.BamExtension}"
    val baiPath = storageDir / s"$randomId${BamExtensions.BaiExtensionAddition}"
    val key = BamKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = BamMetadata(
      bamPath = Some(bamPath.uri),
      baiPath = Some(baiPath.uri),
      notes = existingNote
    )

    Seq(
      (bamPath, bamContents),
      (baiPath, baiContents)
    ).map {
      case (path, contents) => path.write(contents)
    }

    val result = for {
      _ <- runUpsertBam(key, metadata)
      _ <- runIgnore(
        ClioCommand.markExternalBamName,
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
        ClioCommand.queryBamName,
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
        val _ = Seq(bamPath, baiPath)
          .map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "mark bams as External when marking external" in {
    testExternalBam()
  }

  it should "require a note when marking a bam as External" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.markExternalBamName,
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

  it should "preserve existing notes when marking bams as External" in testExternalBam(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  def testDeleteBam(
    existingNote: Option[String] = None,
    testNonExistingFile: Boolean = false,
    force: Boolean = false,
    workspaceName: Option[String] = None
  ): Future[Assertion] = {
    val deleteNote = s"$randomId --- Deleted by the integration tests --- $randomId"

    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val bamContents = s"$randomId --- I am a bam fated to die --- $randomId"
    val baiContents = s"$randomId --- I am an index fated to die --- $randomId"

    val storageDir = rootTestStorageDir / s"bam/$project/$sample/v$version/"
    val bamPath = storageDir / s"$randomId${BamExtensions.BamExtension}"
    val baiPath = storageDir / s"$randomId${BamExtensions.BaiExtensionAddition}"

    val key = BamKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = BamMetadata(
      bamPath = Some(bamPath.uri),
      baiPath = Some(baiPath.uri),
      notes = existingNote,
      workspaceName = workspaceName
    )

    val _ = if (!testNonExistingFile) {
      Seq(
        (bamPath, bamContents),
        (baiPath, baiContents)
      ).map {
        case (path, contents) => path.write(contents)
      }
    }

    val result = for {
      _ <- runUpsertBam(key, metadata)
      _ <- runIgnore(
        ClioCommand.deleteBamName,
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
        ClioCommand.queryBamName,
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
      Seq(bamPath, baiPath).foreach(_ shouldNot exist)

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
        val _ = Seq(bamPath, baiPath)
          .map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "delete bams in GCP along with their bais" in testDeleteBam()

  it should "preserve existing notes when deleting bams" in testDeleteBam(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  it should "not delete bams without a note" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.deleteBamName,
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
      testDeleteBam(workspaceName = Some("testWorkspace"))
    }
  }

  it should "throw an exception when trying to delete a bam if a file does not exist" in {
    recoverToSucceededIf[Exception] {
      testDeleteBam(testNonExistingFile = true)
    }
  }

  it should "delete a bam if a file does not exist and force is true" in testDeleteBam(
    testNonExistingFile = true,
    force = true
  )

  it should "not fail delivery if the bam is already in its target location" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val bamContents = s"$randomId --- I am a dummy bam --- $randomId"
    val baiContents = s"$randomId --- I am a dummy bai --- $randomId"
    val md5Contents = randomId

    val bamName = s"$sample${BamExtensions.BamExtension}"
    val baiName = s"$bamName${BamExtensions.BaiExtensionAddition}"
    val md5Name = s"$bamName${BamExtensions.Md5ExtensionAddition}"

    val rootSource = rootTestStorageDir / s"bam/$project/$sample/v$version/"
    val bamSource = rootSource / bamName
    val baiSource = rootSource / baiName

    val md5Destination = rootSource / md5Name

    val key = BamKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = BamMetadata(
      bamPath = Some(bamSource.uri),
      baiPath = Some(baiSource.uri),
      bamMd5 = Some(Symbol(md5Contents)),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    val _ = Seq((bamSource, bamContents), (baiSource, baiContents)).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertBam(key, metadata)
      _ <- runIgnore(
        ClioCommand.deliverBamName,
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
        ClioCommand.queryBamName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(bamSource, baiSource, md5Destination).foreach(_ should exist)

      Seq(
        (bamSource, bamContents),
        (baiSource, baiContents),
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
        val _ = Seq(bamSource, baiSource, md5Destination)
          .map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "fail delivery if the underlying move fails" in {
    val project = s"project$randomId"
    val sample = s"sample$randomId"
    val version = 3

    val md5Contents = randomId

    val bamName = s"$sample${BamExtensions.BamExtension}"
    val baiName = s"$bamName${BamExtensions.BaiExtensionAddition}"

    val rootSource = rootTestStorageDir / s"bam/$project/$sample/v$version/"
    val bamSource = rootSource / bamName
    val baiSource = rootSource / baiName

    val rootDestination = rootSource.parent / s"moved/$randomId/"

    val key = BamKey(Location.GCP, project, DataType.WGS, sample, version)
    val metadata = BamMetadata(
      bamPath = Some(bamSource.uri),
      baiPath = Some(baiSource.uri),
      bamMd5 = Some(Symbol(md5Contents))
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    recoverToExceptionIf[Exception] {
      for {
        _ <- runUpsertBam(key, metadata)
        // Should fail because the source files don't exist.
        _ <- runIgnore(
          ClioCommand.deliverBamName,
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
          ClioCommand.queryBamName,
          "--workspace-name",
          workspaceName
        )
      } yield {
        // The CLP shouldn't have tried to upsert the workspace name.
        outputs shouldBe empty
      }
    }
  }

  it should "upsert a new bam if force is false" in {
    val upsertKey = BamKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertBam(
        upsertKey,
        BamMetadata(notes = Some("I'm a note")),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Bam)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
    }
  }

  it should "allow an upsert that modifies values not already set or are unchanged if force is false" in {
    val upsertKey = BamKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertBam(
        upsertKey,
        BamMetadata(notes = Some("I'm a note")),
        force = false
      )
      upsertId2 <- runUpsertBam(
        upsertKey,
        BamMetadata(notes = Some("I'm a note"), bamSize = Some(12345)),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Bam)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Bam)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
      storedDocument2.unsafeGet[Long]("bam_size") should be(12345)
    }
  }

  it should "not allow an upsert that modifies values already set if force is false" in {
    val upsertKey = BamKey(
      location = Location.GCP,
      project = s"project$randomId",
      sampleAlias = s"sample$randomId",
      version = 1,
      dataType = DataType.WGS
    )
    for {
      upsertId1 <- runUpsertBam(
        upsertKey,
        BamMetadata(notes = Some("I'm a note")),
        force = false
      )
      _ <- recoverToSucceededIf[Exception] {
        runUpsertBam(
          upsertKey,
          BamMetadata(
            notes = Some("I'm a different note")
          ),
          force = false
        )
      }
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Bam)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
    }
  }
}
