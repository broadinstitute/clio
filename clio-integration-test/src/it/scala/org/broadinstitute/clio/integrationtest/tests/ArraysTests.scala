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
import org.broadinstitute.clio.transfer.model.arrays.{
  ArraysExtensions,
  ArraysKey,
  ArraysMetadata
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}
import org.scalatest.Assertion

import scala.concurrent.Future

trait ArraysTests { self: BaseIntegrationSpec =>
  import ElasticsearchUtil.JsonOps

  def runUpsertArrays(
    key: ArraysKey,
    metadata: ArraysMetadata,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runClient(
      ClioCommand.addArraysName,
      Seq(
        "--location",
        key.location.entryName,
        "--chipwell-barcode",
        key.chipwellBarcode.toString(),
        "--version",
        key.version.toString,
        "--metadata-location",
        tmpMetadata.toString,
        if (force) "--force" else ""
      ).filter(_.nonEmpty): _*
    ).mapTo[UpsertId]
  }

  it should "create the expected arrays mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

    val expected = ElasticsearchIndex.Arrays
    val getRequest = getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
  }

  // Generate a test for every possible Location value.
  Location.values.foreach {
    it should behave like testArraysLocation(_)
  }

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testArraysLocation(location: Location): Unit = {
    it should s"handle upserts and queries for arrays location $location" in {
      val key = ArraysKey(
        location = location,
        chipwellBarcode = Symbol("test chipwellBarcode"),
        version = 2
      )
      val metadata = ArraysMetadata(
        documentStatus = Some(DocumentStatus.Normal),
        chipType = Some("test chipType")
      )
      val expected = expectedMerge(key, metadata)

      for {
        returnedUpsertId <- runUpsertArrays(key, metadata.copy(documentStatus = None))
        outputs <- runClientGetJsonAs[Seq[Json]](
          ClioCommand.queryArraysName,
          "--chipwell-barcode",
          key.chipwellBarcode.toString()
        )
      } yield {
        outputs should contain only expected
        val storedDocument = getJsonFrom(returnedUpsertId)(ElasticsearchIndex.Arrays)
        storedDocument.mapObject(
          _.filterKeys(!ElasticsearchIndex.BookkeepingNames.contains(_))
        ) should be(expected)
      }
    }
  }

  it should "assign different upsertIds to different arrays upserts" in {
    val upsertKey = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"chipwellBarcode$randomId"),
      version = 1
    )

    for {
      upsertId1 <- runUpsertArrays(
        upsertKey,
        ArraysMetadata(
          chipType = Some("test chipType")
        )
      )
      upsertId2 <- runUpsertArrays(
        upsertKey,
        ArraysMetadata(
          chipType = Some("test chipType2")
        )
      )
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Arrays)
      storedDocument1.unsafeGet[URI]("chip_type") should be("test chipType")

      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Arrays)
      storedDocument2.unsafeGet[URI]("chip_type") should be("test chipType2")

      storedDocument1.deepMerge {
        Json.obj(
          ElasticsearchIndex.UpsertIdElasticsearchName -> upsertId2.asJson,
          "chip_type" -> "test chipType2".asJson
        )
      } should be(storedDocument2)
    }
  }

  it should "assign different upsertIds to equal arrays upserts" in {
    val upsertKey = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"chipwell barcode$randomId"),
      version = 1
    )
    val upsertData = ArraysMetadata(
      chipType = Some("chip type")
    )

    for {
      upsertId1 <- runUpsertArrays(upsertKey, upsertData)
      upsertId2 <- runUpsertArrays(upsertKey, upsertData)
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Arrays)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Arrays)
      storedDocument1.mapObject(
        _.add(ElasticsearchIndex.UpsertIdElasticsearchName, upsertId2.asJson)
      ) should be(storedDocument2)
    }
  }

  it should "handle querying arrays by sample and project" in {
    val location = Location.GCP

    val barcodes = {
      val sameId = Symbol(s"barcode$randomId")
      Seq(sameId, sameId, Symbol(s"barcode$randomId"))
    }

    val upserts = Future.sequence {
      barcodes.zip(1 to 3).map {
        case (barcode, version) =>
          val key = ArraysKey(location, barcode, version)
          val data = ArraysMetadata(
            chipType = Some("chip type"),
            isZcalled = Some(true)
          )
          runUpsertArrays(key, data)
      }
    }

    for {
      _ <- upserts
      barcodeResults <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        barcodes.head.toString()
      )
    } yield {
      barcodeResults should have length 2
      barcodeResults.foldLeft(succeed) { (_, result) =>
        result.unsafeGet[String]("chipwell_barcode") should be(barcodes.head)
      }
    }
  }

  it should "only return exact matches for string queries" in {
    val location = Location.GCP

    val prefix = s"testSample$randomId"
    val suffix = s"${randomId}testSample"
    val barcodes = Seq(Symbol(prefix), Symbol(suffix), Symbol(s"$prefix-$suffix"))

    val upserts = Future.sequence {
      barcodes.zip(1 to 3).map {
        case (barcode, version) =>
          val key = ArraysKey(location, barcode, version)
          val data = ArraysMetadata(
            chipType = Some("chip type"),
            isZcalled = Some(true)
          )
          runUpsertArrays(key, data)
      }
    }

    for {
      _ <- upserts
      prefixResults <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        prefix
      )
      suffixResults <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        suffix
      )
    } yield {
      prefixResults should have length 1
      suffixResults should have length 1
    }
  }

  it should "handle updates to arrays metadata" in {
    val barcode = s"testBarcode$randomId"
    val key = ArraysKey(Location.GCP, Symbol(barcode), 1)
    val chipType = "chip type"
    val isZcalled = true
    val initialNotes = "Breaking news"
    val metadata = ArraysMetadata(
      chipType = Some(chipType),
      isZcalled = Some(isZcalled),
      notes = Some(initialNotes)
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[Json]](
          ClioCommand.queryArraysName,
          "--chipwell-barcode",
          barcode
        )
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = ArraysMetadata(
      chipType = metadata.chipType,
      isZcalled = metadata.isZcalled
    )

    for {
      _ <- runUpsertArrays(key, upsertData)
      original <- query
      _ = original.unsafeGet[URI]("chip_type") should be(chipType)
      _ = original.unsafeGet[Boolean]("is_zcalled") should be(isZcalled)
      _ = original.unsafeGet[Option[String]]("notes") should be(None)

      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertArrays(key, upsertData2)
      withNotes <- query
      _ = withNotes.unsafeGet[URI]("chip_type") should be(chipType)
      _ = withNotes.unsafeGet[Boolean]("is_zcalled") should be(isZcalled)
      _ = withNotes.unsafeGet[String]("notes") should be(initialNotes)

      _ <- runUpsertArrays(
        key,
        upsertData2.copy(isZcalled = Some(false), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.unsafeGet[URI]("chip_type") should be(chipType)
      emptyNotes.unsafeGet[Boolean]("is_zcalled") should be(false)
      emptyNotes.unsafeGet[String]("notes") should be("")
    }
  }

  it should "show deleted arrays records on queryAll, but not query" in {
    val barcode = "barcode." + randomId

    val keysWithMetadata = (1 to 3).map { version =>
      val upsertKey = ArraysKey(
        location = Location.GCP,
        chipwellBarcode = Symbol(barcode),
        version = version
      )
      val upsertMetadata = ArraysMetadata(chipType = Some("chip type"))
      (upsertKey, upsertMetadata)
    }
    val (deleteKey, deleteData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (v1Key, metadata) => runUpsertArrays(v1Key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        results <- runClientGetJsonAs[Seq[Json]](
          ClioCommand.queryArraysName,
          "--chipwell-barcode",
          barcode
        )
      } yield {
        results.length should be(expectedLength)
        results.foreach { result =>
          result.unsafeGet[String]("chipwell_barcode") should be(barcode)
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
      _ <- runUpsertArrays(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        barcode,
        "--include-deleted"
      )
    } yield {
      results.length should be(keysWithMetadata.length)
      results.foldLeft(succeed) { (_, result) =>
        result.unsafeGet[String]("chipwell_barcode") should be(barcode)

        val resultKey = ArraysKey(
          location = result.unsafeGet[Location]("location"),
          chipwellBarcode = result.unsafeGet[Symbol]("chipwell_baroce"),
          version = result.unsafeGet[Int]("version")
        )

        result.unsafeGet[DocumentStatus]("document_status") should be {
          if (resultKey == deleteKey) DocumentStatus.Deleted else DocumentStatus.Normal
        }
      }
    }
  }

  def testMove(changeBasename: Boolean = false): Future[Assertion] = {

    val barcode = s"barcode$randomId"
    val version = 3

    val vcfContents = s"$randomId --- I am a dummy vcf --- $randomId"
    val vcfIndexContents = s"$randomId --- I am a dummy vcfIndex --- $randomId"
    val fingerprintingDetailMetricsContents =
      s"$randomId --- I am dummy fingerprintingDetail metrics --- $randomId"
    val fingerprintingSummaryMetricsContents =
      s"$randomId --- I am dummy fingerprintingSummary metrics --- $randomId"

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName =
      s"$vcfName${ArraysExtensions.VcfGzTbiExtension}"
    val fingerprintingDetailMetricsName = s"$randomId.metrics"
    val fingerprintingSummaryMetricsName = s"$randomId.metrics"

    val rootSource = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName
    val fingerprintingDetailMetricsSource = rootSource / fingerprintingDetailMetricsName
    val fingerprintingSummaryMetricsSource = rootSource / fingerprintingSummaryMetricsName

    val endBasename = if (changeBasename) randomId else barcode

    val rootDestination = rootSource.parent / s"moved/$randomId/"
    val vcfDestination = rootDestination / s"$endBasename${ArraysExtensions.VcfGzExtension}"
    val craiDestination = rootDestination / s"$endBasename${ArraysExtensions.VcfGzTbiExtension}"
    val alignmentMetricsDestination = rootDestination / fingerprintingDetailMetricsName
    val fingerprintMetricsDestination = rootDestination / fingerprintingSummaryMetricsName

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      fingerprintingDetailMetricsPath = Some(fingerprintingDetailMetricsSource.uri),
      fingerprintingSummaryMetricsPath = Some(fingerprintingSummaryMetricsSource.uri)
    )

    val _ = Seq(
      (vcfSource, vcfContents),
      (vcfIndexSource, vcfIndexContents),
      (fingerprintingDetailMetricsSource, fingerprintingDetailMetricsContents),
      (fingerprintingSummaryMetricsSource, fingerprintingSummaryMetricsContents)
    ).map {
      case (source, contents) => source.write(contents)
    }

    val args = Seq.concat(
      Seq(
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        barcode,
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
      _ <- runUpsertArrays(key, metadata)
      _ <- runClient(ClioCommand.moveArraysName, args: _*)
    } yield {
      Seq(alignmentMetricsDestination, vcfSource, vcfIndexSource)
        .foreach(_ shouldNot exist)

      Seq(fingerprintingDetailMetricsSource, vcfDestination, craiDestination)
        .foreach(_ should exist)

      // We don't deliver fingerprinting metrics for now because they're based on unpublished research.
      fingerprintingSummaryMetricsSource should exist
      fingerprintMetricsDestination shouldNot exist

      Seq(
        (vcfDestination, vcfContents),
        (craiDestination, vcfIndexContents),
        (fingerprintingDetailMetricsSource, fingerprintingDetailMetricsContents),
        (fingerprintingSummaryMetricsSource, fingerprintingSummaryMetricsContents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
      }
      succeed
    }

    result.andThen {
      case _ =>
        val _ = Seq(
          vcfSource,
          vcfDestination,
          vcfIndexSource,
          craiDestination,
          fingerprintingDetailMetricsSource,
          alignmentMetricsDestination,
          fingerprintingSummaryMetricsSource,
          fingerprintMetricsDestination
        ).map(_.delete(swallowIOExceptions = true))
    }
  }

  it should "move the vcf and vcfIndex together in GCP" in testMove()

  it should "support changing the vcf and vcfIndex basename on move" in testMove(
    changeBasename = true
  )

  it should "not move arrays without a destination" in {
    recoverToExceptionIf[Exception] {
      runClient(
        ClioCommand.moveArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        randomId,
        "--version",
        "123"
      )
    }.map {
      _.getMessage should include("--destination")
    }
  }

  it should "not move arrays with no registered files" in {
    val key = ArraysKey(
      Location.GCP,
      Symbol(s"barcode$randomId"),
      1
    )
    runUpsertArrays(key, ArraysMetadata()).flatMap { _ =>
      recoverToExceptionIf[Exception] {
        runClient(
          ClioCommand.moveArraysName,
          "--location",
          key.location.entryName,
          "--chipwell-barcode",
          key.chipwellBarcode.toString(),
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

  def testDelete(
    existingNote: Option[String] = None,
    testNonExistingFile: Boolean = false,
    force: Boolean = false,
    workspaceName: Option[String] = None
  ): Future[Assertion] = {
    val deleteNote = s"$randomId --- Deleted by the integration tests --- $randomId"

    val barcode = s"barcode$randomId"
    val version = 3

    val vcfContents = s"$randomId --- I am a vcf fated to die --- $randomId"
    val vcfIndexContents = s"$randomId --- I am an index fated to die --- $randomId"
    val metrics1Contents = s"$randomId --- I am an immortal metrics file --- $randomId"
    val metrics2Contents =
      s"$randomId --- I am a second immortal metrics file --- $randomId"

    val storageDir = rootTestStorageDir / s"cram/$barcode/v$version/"
    val vcfPath = storageDir / s"$randomId${ArraysExtensions.VcfGzExtension}"
    val vcfIndexPath = storageDir / s"$randomId${ArraysExtensions.VcfGzTbiExtension}"
    val metrics1Path = storageDir / s"$randomId.metrics"
    val metrics2Path = storageDir / s"$randomId.metrics"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfPath.uri),
      vcfIndexPath = Some(vcfIndexPath.uri),
      fingerprintingDetailMetricsPath = Some(metrics1Path.uri),
      fingerprintingSummaryMetricsPath = Some(metrics1Path.uri),
      notes = existingNote,
      workspaceName = workspaceName
    )

    val _ = if (!testNonExistingFile) {
      Seq(
        (vcfPath, vcfContents),
        (vcfIndexPath, vcfIndexContents),
        (metrics1Path, metrics1Contents),
        (metrics2Path, metrics2Contents)
      ).map {
        case (path, contents) => path.write(contents)
      }
    }

    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runClient(
        ClioCommand.deleteArraysName,
        Seq(
          "--location",
          Location.GCP.entryName,
          "--chipwell-barcode",
          barcode,
          "--version",
          version.toString,
          "--note",
          deleteNote,
          if (force) "--force" else ""
        ).filter(_.nonEmpty): _*
      )
      outputs <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        barcode,
        "--version",
        version.toString,
        "--include-deleted"
      )
    } yield {
      Seq(vcfPath, vcfIndexPath).foreach(_ shouldNot exist)
      if (!testNonExistingFile) {
        Seq((metrics1Path, metrics1Contents), (metrics2Path, metrics2Contents)).foreach {
          case (path, contents) =>
            path should exist
            path.contentAsString should be(contents)
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
      case _ =>
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Seq(vcfPath, vcfIndexPath, metrics1Path, metrics2Path)
          .map(_.delete(swallowIOExceptions = true))
    }
  }

  it should "delete vcf in GCP along with its index, but not metrics" in testDelete()

  it should "preserve existing notes when deleting arrays" in testDelete(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  it should "not delete arrays without a note" in {
    recoverToExceptionIf[Exception] {
      runClient(
        ClioCommand.deleteArraysName,
        "--location",
        Location.GCP.entryName,
        "--project",
        randomId,
        "--chipwell-barcode",
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
      testDelete(workspaceName = Some("testWorkspace"))
    }
  }

  it should "throw an exception when trying to delete if a file does not exist" in {
    recoverToSucceededIf[Exception] {
      testDelete(testNonExistingFile = true)
    }
  }

  it should "delete if a file does not exist and force is true" in testDelete(
    testNonExistingFile = true,
    force = true
  )

  it should "move files and record the workspace name when delivering" in {
    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfContents = s"$id --- I am a dummy vcf --- $id"
    val vcfIndexContents = s"$id --- I am a dummy vcfIndex --- $id"

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$vcfName${ArraysExtensions.VcfGzTbiExtension}"

    val rootSource = rootTestStorageDir / s"cram/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName

    val prefix = "new_basename_"
    val newBasename = s"$prefix$barcode"
    val rootDestination = rootSource.parent / s"moved/$id/"
    val vcfDestination = rootDestination / s"$prefix$vcfName"
    val vcfIndexDestination = rootDestination / s"$prefix$vcfIndexName"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$id-TestWorkspace-$id"

    val _ = Seq((vcfSource, vcfContents), (vcfIndexSource, vcfIndexContents)).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runClient(
        ClioCommand.deliverArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        barcode,
        "--version",
        version.toString,
        "--workspace-name",
        workspaceName,
        "--workspace-path",
        rootDestination.uri.toString,
        "--new-basename",
        newBasename
      )
      outputs <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryArraysName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(vcfSource, vcfIndexSource).foreach(_ shouldNot exist)

      Seq(vcfDestination, vcfIndexDestination).foreach(_ should exist)

      Seq(
        (vcfDestination, vcfContents),
        (vcfIndexDestination, vcfIndexContents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
      }

      outputs should contain only expectedMerge(
        key,
        metadata.copy(
          workspaceName = Some(workspaceName),
          vcfPath = Some(vcfDestination.uri),
          vcfIndexPath = Some(vcfIndexDestination.uri)
        )
      )
    }

    result.andThen {
      case _ =>
        val _ = Seq(
          vcfSource,
          vcfDestination,
          vcfIndexSource,
          vcfIndexDestination
        ).map(_.delete(swallowIOExceptions = true))
    }
  }

  it should "not fail delivery if the vcf is already in its target location" in {
    val barcode = s"barcode$randomId"
    val version = 3

    val vcfContents = s"$randomId --- I am a dummy vcf --- $randomId"
    val vcfIncexContents = s"$randomId --- I am a dummy vcfIndex --- $randomId"

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$vcfName${ArraysExtensions.VcfGzTbiExtension}"

    val rootSource = rootTestStorageDir / s"cram/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    val _ = Seq((vcfSource, vcfContents), (vcfIndexSource, vcfIncexContents)).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runClient(
        ClioCommand.deliverArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        barcode,
        "--version",
        version.toString,
        "--workspace-name",
        workspaceName,
        "--workspace-path",
        rootSource.uri.toString
      )
      outputs <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryArraysName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(vcfSource, vcfIndexSource).foreach(_ should exist)

      Seq(
        (vcfSource, vcfContents),
        (vcfIndexSource, vcfIncexContents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
      }

      outputs should contain only expectedMerge(
        key,
        metadata.copy(workspaceName = Some(workspaceName))
      )
    }

    result.andThen {
      case _ =>
        val _ = Seq(vcfSource, vcfIndexSource)
          .map(_.delete(swallowIOExceptions = true))
    }
  }

  it should "fail delivery if the underlying move fails" in {
    val barcode = s"barcode$randomId"
    val version = 3

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$vcfName${ArraysExtensions.VcfGzTbiExtension}"

    val rootSource = rootTestStorageDir / s"cram/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName

    val rootDestination = rootSource.parent / s"moved/$randomId/"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri)
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    recoverToExceptionIf[Exception] {
      for {
        _ <- runUpsertArrays(key, metadata)
        // Should fail because the source files don't exist.
        deliverResponse <- runClient(
          ClioCommand.deliverArraysName,
          "--location",
          Location.GCP.entryName,
          "--chipwell-barcode",
          barcode,
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
        outputs <- runClientGetJsonAs[Seq[Json]](
          ClioCommand.queryArraysName,
          "--workspace-name",
          workspaceName
        )
      } yield {
        // The CLP shouldn't have tried to upsert the workspace name.
        outputs shouldBe empty
      }
    }
  }

  it should "upsert a new arrays if force is false" in {
    val upsertKey = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"barcode$randomId"),
      version = 1
    )
    for {
      upsertId1 <- runUpsertArrays(
        upsertKey,
        ArraysMetadata(notes = Some("I'm a note")),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Arrays)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
    }
  }

  it should "allow an upsert that modifies values not already set or are unchanged if force is false" in {
    val upsertKey = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"barcode$randomId"),
      version = 1
    )
    for {
      upsertId1 <- runUpsertArrays(
        upsertKey,
        ArraysMetadata(notes = Some("I'm a note")),
        force = false
      )
      upsertId2 <- runUpsertArrays(
        upsertKey,
        ArraysMetadata(notes = Some("I'm a note"), isZcalled = Some(true)),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Arrays)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Arrays)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
      storedDocument2.unsafeGet[Boolean]("is_zcalled") should be(true)
    }
  }

  it should "not allow an upsert that modifies values already set if force is false" in {
    val upsertKey = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"barcode$randomId"),
      version = 1
    )
    for {
      upsertId1 <- runUpsertArrays(
        upsertKey,
        ArraysMetadata(notes = Some("I'm a note")),
        force = false
      )
      _ <- recoverToSucceededIf[Exception] {
        runUpsertArrays(
          upsertKey,
          ArraysMetadata(
            notes = Some("I'm a different note")
          ),
          force = false
        )
      }
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Arrays)
      storedDocument1.unsafeGet[String]("notes") should be("I'm a note")
    }
  }
}
