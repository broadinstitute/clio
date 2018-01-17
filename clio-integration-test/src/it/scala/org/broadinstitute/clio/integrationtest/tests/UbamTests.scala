package org.broadinstitute.clio.integrationtest.tests

import java.net.URI
import java.nio.file.Files

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import com.sksamuel.elastic4s.IndexAndType
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient.FailedResponse
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentWgsUbam,
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata,
  TransferUbamV1QueryOutput,
  UbamExtensions
}
import org.broadinstitute.clio.util.model._
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests of Clio's ubam functionality. */
trait UbamTests { self: BaseIntegrationSpec =>

  def runUpsertUbam(
    key: TransferUbamV1Key,
    metadata: TransferUbamV1Metadata,
    sequencingType: SequencingType
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    val command = sequencingType match {
      case SequencingType.WholeGenome     => ClioCommand.addWgsUbamName
      case SequencingType.HybridSelection => ClioCommand.addHybselUbamName
    }
    runClient(
      command,
      "--flowcell-barcode",
      key.flowcellBarcode,
      "--lane",
      key.lane.toString,
      "--library-name",
      key.libraryName,
      "--location",
      key.location.entryName,
      "--metadata-location",
      tmpMetadata.toString
    ).mapTo[UpsertId]
  }

  def statusCodeShouldBe(expectedStatusCode: StatusCode): FailedResponse => Assertion = {
    e: FailedResponse =>
      e.statusCode should be(expectedStatusCode)
  }

  it should "throw a FailedResponse 404 when running get schema command for hybsel ubams" in {
    val getSchemaResponseFuture = runClient(ClioCommand.getHybselUbamSchemaName)
    recoverToExceptionIf[FailedResponse](getSchemaResponseFuture)
      .map(statusCodeShouldBe(StatusCodes.NotFound))
  }

  val stubKey = TransferUbamV1Key(
    Location.GCP,
    "fake_barcode",
    2,
    "fake_library"
  )

  it should "throw a FailedResponse 404 when running add command for hybsel ubams" in {
    val tmpMetadata =
      writeLocalTmpJson(TransferUbamV1Metadata(project = Some("fake_project")))

    val addResponseFuture = runClient(
      ClioCommand.addHybselUbamName,
      "--flowcell-barcode",
      stubKey.flowcellBarcode,
      "--lane",
      stubKey.lane.toString,
      "--library-name",
      stubKey.libraryName,
      "--location",
      stubKey.location.entryName,
      "--metadata-location",
      tmpMetadata.toString
    )
    recoverToExceptionIf[FailedResponse](addResponseFuture)
      .map(statusCodeShouldBe(StatusCodes.NotFound))
  }

  it should "throw a FailedResponse 404 when running query command for hybsel ubams" in {
    val queryResponseFuture = runClient(
      ClioCommand.queryHybselUbamName,
      "--flowcell-barcode",
      stubKey.flowcellBarcode
    )
    recoverToExceptionIf[FailedResponse](queryResponseFuture)
      .map(statusCodeShouldBe(StatusCodes.NotFound))
  }

  def messageShouldBe(expectedMessage: String): Exception => Assertion = { e: Exception =>
    e.getMessage should be(expectedMessage)
  }

  it should "throw a FailedResponse 404 when running move command for hybsel ubams" in {
    val moveResponseFuture = runClient(
      ClioCommand.moveHybselUbamName,
      "--flowcell-barcode",
      stubKey.flowcellBarcode,
      "--lane",
      stubKey.lane.toString,
      "--library-name",
      stubKey.libraryName,
      "--location",
      stubKey.location.entryName,
      "--destination",
      "gs://fake-path/"
    )
    recoverToExceptionIf[Exception](moveResponseFuture)
      .map(messageShouldBe("Could not query the HybselUbam. No files have been moved."))
  }

  it should "throw a FailedResponse 404 when running delete command for hybsel ubams" in {
    val deleteResponseFuture = runClient(
      ClioCommand.deleteHybselUbamName,
      "--flowcell-barcode",
      stubKey.flowcellBarcode,
      "--lane",
      stubKey.lane.toString,
      "--library-name",
      stubKey.libraryName,
      "--location",
      stubKey.location.entryName,
      "--note",
      "note"
    )
    recoverToExceptionIf[Exception](deleteResponseFuture)
      .map(messageShouldBe("Could not query the HybselUbam. No files have been deleted."))
  }

  it should "create the expected wgs-ubam mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

    val expected = ElasticsearchIndex.WgsUbam
    val getRequest =
      getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
  }

  it should "report the expected JSON schema for wgs-ubams" in {
    runClient(ClioCommand.getWgsUbamSchemaName)
      .map(_ should be(WgsUbamIndex.jsonSchema))
  }

  // Generate a test for every possible Location value.
  Location.values.foreach {
    it should behave like testWgsUbamLocation(_)
  }

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testWgsUbamLocation(location: Location): Unit = {
    val expected = TransferUbamV1QueryOutput(
      flowcellBarcode = "barcode2",
      lane = 2,
      libraryName = s"library $randomId",
      location = location,
      project = Some("test project"),
      documentStatus = Some(DocumentStatus.Normal)
    )

    /*
     * NOTE: This is lazy on purpose. If it executes outside of the actual `it` block,
     * it'll result in an `UninitializedFieldError` because the spec `beforeAll` won't
     * have triggered yet.
     */
    lazy val responseFuture = runUpsertUbam(
      TransferUbamV1Key(
        expected.location,
        expected.flowcellBarcode,
        expected.lane,
        expected.libraryName
      ),
      TransferUbamV1Metadata(project = expected.project),
      SequencingType.WholeGenome
    )

    it should s"handle upserts and queries for wgs-ubam location $location" in {
      for {
        returnedUpsertId <- responseFuture
        queryResponse <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
          ClioCommand.queryWgsUbamName,
          "--library-name",
          expected.libraryName
        )
      } yield {
        queryResponse should be(Seq(expected))

        val storedDocument = getJsonFrom[DocumentWgsUbam](
          ElasticsearchIndex.WgsUbam,
          returnedUpsertId
        )
        storedDocument.flowcellBarcode should be(expected.flowcellBarcode)
        storedDocument.lane should be(expected.lane)
        storedDocument.libraryName should be(expected.libraryName)
        storedDocument.location should be(expected.location)
        storedDocument.project should be(expected.project)
      }
    }
  }

  it should "assign different upsertIds to different wgs-ubam upserts" in {
    val upsertKey = TransferUbamV1Key(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )

    for {
      upsertId1 <- runUpsertUbam(
        upsertKey,
        TransferUbamV1Metadata(project = Some("testProject1")),
        SequencingType.WholeGenome
      )
      upsertId2 <- runUpsertUbam(
        upsertKey,
        TransferUbamV1Metadata(project = Some("testProject2")),
        SequencingType.WholeGenome
      )
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 =
        getJsonFrom[DocumentWgsUbam](ElasticsearchIndex.WgsUbam, upsertId1)
      storedDocument1.project should be(Some("testProject1"))

      val storedDocument2 =
        getJsonFrom[DocumentWgsUbam](ElasticsearchIndex.WgsUbam, upsertId2)
      storedDocument2.project should be(Some("testProject2"))

      storedDocument1.copy(upsertId = upsertId2, project = Some("testProject2")) should be(
        storedDocument2
      )
    }
  }

  it should "assign different upsertIds to equal wgs-ubam upserts" in {
    val upsertKey = TransferUbamV1Key(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )
    val metadata = TransferUbamV1Metadata(project = Some("testProject1"))
    val ubamType = SequencingType.WholeGenome

    for {
      upsertId1 <- runUpsertUbam(upsertKey, metadata, ubamType)
      upsertId2 <- runUpsertUbam(upsertKey, metadata, ubamType)
    } yield {
      upsertId2.compareTo(upsertId1) > 0 should be(true)

      val storedDocument1 =
        getJsonFrom[DocumentWgsUbam](ElasticsearchIndex.WgsUbam, upsertId1)
      val storedDocument2 =
        getJsonFrom[DocumentWgsUbam](ElasticsearchIndex.WgsUbam, upsertId2)
      storedDocument1.copy(upsertId = upsertId2) should be(storedDocument2)
    }
  }

  it should "handle querying wgs-ubams by sample, project, and research-project-id" in {
    val flowcellBarcode = "barcode2"
    val lane = 2
    val location = Location.GCP
    val project = "testProject" + randomId

    val libraries = Seq.fill(3)("library" + randomId)
    val samples = {
      val sameId = "testSample" + randomId
      Seq(sameId, sameId, "testSample" + randomId)
    }
    val researchProjectIds = Seq.fill(3)("rpId" + randomId)

    val upserts = Future.sequence {
      Seq(libraries, samples, researchProjectIds).transpose.map {
        case Seq(library, sample, researchProjectId) =>
          val key =
            TransferUbamV1Key(location, flowcellBarcode, lane, library)
          val metadata = TransferUbamV1Metadata(
            project = Some(project),
            sampleAlias = Some(sample),
            researchProjectId = Some(researchProjectId)
          )
          runUpsertUbam(key, metadata, SequencingType.WholeGenome)
      }
    }

    for {
      _ <- upserts
      projectResults <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
        ClioCommand.queryWgsUbamName,
        "--project",
        project
      )
      sampleResults <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
        ClioCommand.queryWgsUbamName,
        "--sample-alias",
        samples.head
      )
      rpIdResults <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
        ClioCommand.queryWgsUbamName,
        "--research-project-id",
        researchProjectIds.last
      )
    } yield {
      projectResults should have length 3
      projectResults.foldLeft(succeed) { (_, result) =>
        result.project should be(Some(project))
      }
      sampleResults should have length 2
      sampleResults.foldLeft(succeed) { (_, result) =>
        result.sampleAlias should be(Some(samples.head))
      }
      rpIdResults should have length 1
      rpIdResults.headOption.flatMap(_.researchProjectId) should be(
        researchProjectIds.lastOption
      )
    }
  }

  it should "handle updates to wgs-ubam metadata" in {
    val key =
      TransferUbamV1Key(Location.GCP, "barcode2", 2, s"library$randomId")
    val project = s"testProject$randomId"
    val metadata = TransferUbamV1Metadata(
      project = Some(project),
      sampleAlias = Some("sampleAlias1"),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
          ClioCommand.queryWgsUbamName,
          "--project",
          project
        )
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = TransferUbamV1Metadata(
      sampleAlias = metadata.sampleAlias,
      project = metadata.project
    )

    for {
      _ <- runUpsertUbam(key, upsertData, SequencingType.WholeGenome)
      original <- query
      _ = original.sampleAlias should be(metadata.sampleAlias)
      _ = original.notes should be(None)
      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertUbam(key, upsertData2, SequencingType.WholeGenome)
      withNotes <- query
      _ = withNotes.sampleAlias should be(metadata.sampleAlias)
      _ = withNotes.notes should be(metadata.notes)
      _ <- runUpsertUbam(
        key,
        upsertData2.copy(sampleAlias = Some("sampleAlias2"), notes = Some("")),
        SequencingType.WholeGenome
      )
      emptyNotes <- query
    } yield {
      emptyNotes.sampleAlias should be(Some("sampleAlias2"))
      emptyNotes.notes should be(Some(""))
    }
  }

  it should "show deleted records on queryall, but not query" in {
    val barcode = "fc5440"
    val project = "testProject" + randomId
    val sample = "sample688." + randomId
    val keysWithMetadata = (1 to 3).map { lane =>
      val upsertKey = TransferUbamV1Key(
        flowcellBarcode = barcode,
        lane = lane,
        libraryName = "library" + randomId,
        location = Location.GCP
      )
      val upsertMetadata = TransferUbamV1Metadata(
        project = Some(project),
        sampleAlias = Some(sample),
        ubamPath = Some(URI.create(s"gs://ubam/$sample.$lane"))
      )
      (upsertKey, upsertMetadata)
    }
    val (deleteKey, deleteData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (key, metadata) => runUpsertUbam(key, metadata, SequencingType.WholeGenome)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        results <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
          ClioCommand.queryWgsUbamName,
          "--project",
          project,
          "--flowcell-barcode",
          barcode
        )
      } yield {
        results.length should be(expectedLength)
        results.foreach { result =>
          result.project should be(Some(project))
          result.sampleAlias should be(Some(sample))
          result.documentStatus should be(Some(DocumentStatus.Normal))
        }
        results
      }
    }

    for {
      _ <- upserts
      _ <- checkQuery(expectedLength = 3)
      _ <- runUpsertUbam(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted)),
        SequencingType.WholeGenome
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
        ClioCommand.queryWgsUbamName,
        "--project",
        project,
        "--flowcell-barcode",
        barcode,
        "--include-deleted"
      )
    } yield {
      results.length should be(keysWithMetadata.length)
      results.foldLeft(succeed) { (_, result) =>
        result.project should be(Some(project))
        result.sampleAlias should be(Some(sample))

        val resultKey = TransferUbamV1Key(
          flowcellBarcode = result.flowcellBarcode,
          lane = result.lane,
          libraryName = result.libraryName,
          location = result.location
        )

        if (resultKey == deleteKey) {
          result.documentStatus should be(Some(DocumentStatus.Deleted))
        } else {
          result.documentStatus should be(Some(DocumentStatus.Normal))
        }
      }
    }
  }

  def testMoveUbam(srcIsDest: Boolean = false): Future[Assertion] = {
    val barcode = s"barcode$randomId"
    val lane = 3
    val library = s"lib$randomId"

    val fileContents = s"$randomId --- I am a dummy ubam --- $randomId"
    val ubamName = s"$randomId${UbamExtensions.UbamExtension}"
    val sourceDir =
      rootTestStorageDir.resolve(s"wgs-ubam/$barcode/$lane/$library/$ubamName/")
    val targetDir = if (srcIsDest) {
      sourceDir
    } else {
      sourceDir.getParent.resolve(s"moved/")
    }
    val sourceUbam = sourceDir.resolve(ubamName)
    val targetUbam = targetDir.resolve(ubamName)

    val key = TransferUbamV1Key(Location.GCP, barcode, lane, library)
    val metadata =
      TransferUbamV1Metadata(ubamPath = Some(sourceUbam.toUri))

    // Clio needs the metadata to be added before it can be moved.
    val _ = Files.write(sourceUbam, fileContents.getBytes)
    val result = for {
      _ <- runUpsertUbam(key, metadata, SequencingType.WholeGenome)
      _ <- runClient(
        ClioCommand.moveWgsUbamName,
        "--flowcell-barcode",
        barcode,
        "--lane",
        lane.toString,
        "--library-name",
        library,
        "--location",
        Location.GCP.entryName,
        "--destination",
        targetDir.toUri.toString
      )
    } yield {
      Files.exists(sourceUbam) should be(srcIsDest)
      Files.exists(targetUbam) should be(true)
      new String(Files.readAllBytes(targetUbam)) should be(fileContents)
    }

    result.andThen[Unit] {
      case _ => {
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Seq(sourceUbam, targetUbam).map(Files.deleteIfExists)
      }
    }
  }

  it should "move wgs-ubams in GCP" in testMoveUbam()

  it should "not delete wgs-ubams when moving and source == destination" in testMoveUbam(
    srcIsDest = true
  )

  it should "not move wgs-ubams without a destination" in {
    recoverToExceptionIf[Exception] {
      runClient(
        ClioCommand.moveWgsUbamName,
        "--flowcell-barcode",
        randomId,
        "--lane",
        "123",
        "--library-name",
        randomId,
        "--location",
        Location.GCP.entryName
      )
    }.map {
      _.getMessage should include("--destination")
    }
  }

  it should "not move wgs-ubams when the source file doesn't exist" in {
    val barcode = s"barcode$randomId"
    val lane = 3
    val library = s"lib$randomId"

    val cloudPath = rootTestStorageDir.resolve(
      s"wgs-ubam/$barcode/$lane/$library/$randomId${UbamExtensions.UbamExtension}"
    )
    val cloudPath2 =
      cloudPath.getParent.resolve(
        s"moved/$randomId${UbamExtensions.UbamExtension}"
      )

    val key = TransferUbamV1Key(Location.GCP, barcode, lane, library)
    val metadata =
      TransferUbamV1Metadata(ubamPath = Some(cloudPath.toUri))

    val result = for {
      _ <- runUpsertUbam(key, metadata, SequencingType.WholeGenome)
      _ <- recoverToSucceededIf[Exception] {
        runClient(
          ClioCommand.moveWgsUbamName,
          "--flowcell-barcode",
          barcode,
          "--lane",
          lane.toString,
          "--library-name",
          library,
          "--location",
          Location.GCP.entryName,
          "--destination",
          cloudPath2.toUri.toString
        )
      }
      queryOutputs <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
        ClioCommand.queryWgsUbamName,
        "--flowcell-barcode",
        barcode,
        "--lane",
        lane.toString,
        "--library-name",
        library,
        "--location",
        Location.GCP.entryName
      )
    } yield {
      Files.exists(cloudPath2) should be(false)
      queryOutputs should have length 1
      queryOutputs.head.ubamPath should be(Some(cloudPath.toUri))
    }

    result.andThen[Unit] {
      case _ => {
        val _ = Files.deleteIfExists(cloudPath2)
      }
    }
  }

  it should "respect user-set regulatory designation for wgs ubams" in {
    val flowcellBarcode = s"testRegulatoryDesignation.$randomId"
    val library = s"library.$randomId"
    val lane = 1
    val regulatoryDesignation = Some(RegulatoryDesignation.ClinicalDiagnostics)
    val upsertKey =
      TransferUbamV1Key(Location.GCP, flowcellBarcode, lane, library)
    val metadata = TransferUbamV1Metadata(
      project = Some("testProject1"),
      regulatoryDesignation = regulatoryDesignation
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
          ClioCommand.queryWgsUbamName,
          "--flowcell-barcode",
          flowcellBarcode
        )
      } yield {
        results should have length 1
        results.head
      }
    }
    for {
      upsert <- runUpsertUbam(upsertKey, metadata, SequencingType.WholeGenome)
      queried <- query
    } yield {
      queried.regulatoryDesignation should be(regulatoryDesignation)
    }
  }

  def testDeleteUbam(existingNote: Option[String] = None): Future[Assertion] = {
    val deleteNote =
      s"$randomId --- Deleted by the integration tests --- $randomId"

    val barcode = s"barcode$randomId"
    val lane = 4
    val library = s"lib$randomId"

    val fileContents = s"$randomId --- I am fated to die --- $randomId"
    val cloudPath = rootTestStorageDir.resolve(
      s"wgs-ubam/$barcode/$lane/$library/$randomId${UbamExtensions.UbamExtension}"
    )

    val key = TransferUbamV1Key(Location.GCP, barcode, lane, library)
    val metadata = TransferUbamV1Metadata(
      ubamPath = Some(cloudPath.toUri),
      notes = existingNote
    )

    // Clio needs the metadata to be added before it can be deleted.
    val _ = Files.write(cloudPath, fileContents.getBytes)
    val result = for {
      _ <- runUpsertUbam(key, metadata, SequencingType.WholeGenome)
      _ <- runClient(
        ClioCommand.deleteWgsUbamName,
        "--flowcell-barcode",
        barcode,
        "--lane",
        lane.toString,
        "--library-name",
        library,
        "--location",
        Location.GCP.entryName,
        "--note",
        deleteNote
      )
      _ = Files.exists(cloudPath) should be(false)
      outputs <- runClientGetJsonAs[Seq[TransferUbamV1QueryOutput]](
        ClioCommand.queryWgsUbamName,
        "--flowcell-barcode",
        barcode,
        "--lane",
        lane.toString,
        "--library-name",
        library,
        "--location",
        Location.GCP.entryName,
        "--include-deleted"
      )
    } yield {
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
        val _ = Files.deleteIfExists(cloudPath)
      }
    }
  }

  it should "delete wgs-ubams in GCP" in testDeleteUbam()

  it should "preserve existing notes when deleting wgs-ubams" in testDeleteUbam(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  it should "not delete wgs-ubams without a note" in {
    recoverToExceptionIf[Exception] {
      runClient(
        ClioCommand.deleteWgsUbamName,
        "--flowcell-barcode",
        randomId,
        "--lane",
        "123",
        "--library-name",
        randomId,
        "--location",
        Location.GCP.entryName
      )
    }.map {
      _.getMessage should include("--note")
    }
  }
}
