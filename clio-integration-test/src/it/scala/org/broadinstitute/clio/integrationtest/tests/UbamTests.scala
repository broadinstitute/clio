package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import io.circe.syntax._
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient.FailedResponse
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.transfer.model.ubam.{UbamExtensions, UbamKey, UbamMetadata}
import org.broadinstitute.clio.util.model._
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests of Clio's ubam functionality. */
trait UbamTests { self: BaseIntegrationSpec =>
  import ElasticsearchUtil.JsonOps

  def runUpsertUbam(
    key: UbamKey,
    metadata: UbamMetadata,
    sequencingType: SequencingType,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    val command = sequencingType match {
      case SequencingType.WholeGenome     => ClioCommand.addWgsUbamName
      case SequencingType.HybridSelection => ClioCommand.addHybselUbamName
    }
    runClient(
      command,
      Seq(
        "--flowcell-barcode",
        key.flowcellBarcode,
        "--lane",
        key.lane.toString,
        "--library-name",
        key.libraryName,
        "--location",
        key.location.entryName,
        "--metadata-location",
        tmpMetadata.toString,
        if (force) "--force" else ""
      ).filter(_.nonEmpty): _*
    ).mapTo[UpsertId]
  }

  def statusCodeShouldBe(expectedStatusCode: StatusCode): FailedResponse => Assertion = {
    e: FailedResponse =>
      e.statusCode should be(expectedStatusCode)
  }

  val stubKey = UbamKey(
    Location.GCP,
    "fake_barcode",
    2,
    "fake_library"
  )

  it should "throw a FailedResponse 404 when running add command for hybsel ubams" in {
    val tmpMetadata =
      writeLocalTmpJson(UbamMetadata(project = Some("fake_project")))

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
    recoverToExceptionIf[FailedResponse](moveResponseFuture)
      .map(statusCodeShouldBe(StatusCodes.NotFound))
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
    recoverToExceptionIf[FailedResponse](deleteResponseFuture)
      .map(statusCodeShouldBe(StatusCodes.NotFound))
  }

  it should "create the expected wgs-ubam mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

    val expected = ElasticsearchIndex.WgsUbam
    val getRequest = getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
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
    it should s"handle upserts and queries for wgs-ubam location $location" in {
      val key = UbamKey(
        location = location,
        flowcellBarcode = "barcode2",
        lane = 2,
        libraryName = s"library $randomId"
      )
      val metadata = UbamMetadata(
        documentStatus = Some(DocumentStatus.Normal),
        project = Some("test project")
      )
      val expected = expectedMerge(key, metadata)

      for {
        returnedUpsertId <- runUpsertUbam(
          key,
          metadata.copy(documentStatus = None),
          SequencingType.WholeGenome
        )
        queryResponse <- runClientGetJsonAs[Seq[Json]](
          ClioCommand.queryWgsUbamName,
          "--library-name",
          key.libraryName
        )
      } yield {
        queryResponse should contain only expected
        val storedDocument = getJsonFrom(returnedUpsertId)(ElasticsearchIndex.WgsUbam)
        storedDocument.mapObject(
          _.filterKeys(!ElasticsearchIndex.BookkeepingNames.contains(_))
        ) should be(expected)
      }
    }
  }

  it should "assign different upsertIds to different wgs-ubam upserts" in {
    val upsertKey = UbamKey(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )

    for {
      upsertId1 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(project = Some("testProject1")),
        SequencingType.WholeGenome
      )
      upsertId2 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(project = Some("testProject2")),
        SequencingType.WholeGenome
      )
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsUbam)
      storedDocument1.unsafeGet[String]("project") should be("testProject1")

      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.WgsUbam)
      storedDocument2.unsafeGet[String]("project") should be("testProject2")

      storedDocument1.deepMerge {
        Json.obj(
          ElasticsearchIndex.UpsertIdElasticsearchName -> upsertId2.asJson,
          "project" -> "testProject2".asJson
        )
      } should be(storedDocument2)
    }
  }

  it should "assign different upsertIds to equal wgs-ubam upserts" in {
    val upsertKey = UbamKey(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )
    val metadata = UbamMetadata(project = Some("testProject1"))
    val ubamType = SequencingType.WholeGenome

    for {
      upsertId1 <- runUpsertUbam(upsertKey, metadata, ubamType)
      upsertId2 <- runUpsertUbam(upsertKey, metadata, ubamType)
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsUbam)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.WgsUbam)

      storedDocument1.mapObject(
        _.add(ElasticsearchIndex.UpsertIdElasticsearchName, upsertId2.asJson)
      ) should be(storedDocument2)
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
          val key = UbamKey(location, flowcellBarcode, lane, library)
          val metadata = UbamMetadata(
            project = Some(project),
            sampleAlias = Some(sample),
            researchProjectId = Some(researchProjectId)
          )
          runUpsertUbam(key, metadata, SequencingType.WholeGenome)
      }
    }

    for {
      _ <- upserts
      projectResults <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryWgsUbamName,
        "--project",
        project
      )
      sampleResults <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryWgsUbamName,
        "--sample-alias",
        samples.head
      )
      rpIdResults <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryWgsUbamName,
        "--research-project-id",
        researchProjectIds.last
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
      rpIdResults should have length 1
      rpIdResults.head.unsafeGet[String]("research_project_id") should be(
        researchProjectIds.last
      )
    }
  }

  it should "handle querying ubams by aggregated-by" in {
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

    val aggregations = Seq(
      AggregatedBy.Squid,
      AggregatedBy.Squid,
      AggregatedBy.RP
    )

    val upserts = Future.sequence {
      libraries zip samples zip researchProjectIds zip aggregations map {
        case (((lib, samp), rpid), agg) => (lib, samp, rpid, agg)
      } map {
        case (library, sample, researchProjectId, aggregation) =>
          val key = UbamKey(location, flowcellBarcode, lane, library)
          val metadata = UbamMetadata(
            project = Some(project),
            sampleAlias = Some(sample),
            researchProjectId = Some(researchProjectId),
            aggregatedBy = Some(aggregation)
          )
          runUpsertUbam(key, metadata, SequencingType.WholeGenome)
      }
    }

    for {
      _ <- upserts
      queryResults <- runClientGetJsonAs[Seq[Json]](
        ClioCommand.queryWgsUbamName,
        "--project",
        project,
        "--aggregated-by",
        AggregatedBy.Squid.entryName
      )
    } yield {
      queryResults should have length 2
      queryResults.foldLeft(succeed) { (_, result) =>
        result.unsafeGet[String]("project") should be(project)
      }
      queryResults.foldLeft(succeed) { (_, result) =>
        researchProjectIds.take(2) should contain(
          result.unsafeGet[String]("research_project_id")
        )
      }
    }
  }

  it should "handle updates to wgs-ubam metadata" in {
    val key = UbamKey(Location.GCP, "barcode2", 2, s"library$randomId")
    val project = s"testProject$randomId"
    val metadata = UbamMetadata(
      project = Some(project),
      sampleAlias = Some("sampleAlias1"),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[Json]](
          ClioCommand.queryWgsUbamName,
          "--project",
          project
        )
      } yield {
        results should have length 1
        results.head
      }
    }

    val upsertData = UbamMetadata(
      project = metadata.project,
      sampleAlias = metadata.sampleAlias
    )

    for {
      _ <- runUpsertUbam(key, upsertData, SequencingType.WholeGenome)
      original <- query
      _ = original.unsafeGet[String]("sample_alias") should be("sampleAlias1")
      _ = original.unsafeGet[Option[String]]("notes") should be(None)

      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertUbam(key, upsertData2, SequencingType.WholeGenome)
      withNotes <- query
      _ = withNotes.unsafeGet[String]("sample_alias") should be("sampleAlias1")
      _ = withNotes.unsafeGet[String]("notes") should be("Breaking news")

      _ <- runUpsertUbam(
        key,
        upsertData2.copy(sampleAlias = Some("sampleAlias2"), notes = Some("")),
        SequencingType.WholeGenome
      )
      emptyNotes <- query
    } yield {
      emptyNotes.unsafeGet[String]("sample_alias") should be("sampleAlias2")
      emptyNotes.unsafeGet[String]("notes") should be("")
    }
  }

  it should "show deleted records on queryall, but not query" in {
    val barcode = "fc5440"
    val project = "testProject" + randomId
    val sample = "sample688." + randomId
    val keysWithMetadata = (1 to 3).map { lane =>
      val upsertKey = UbamKey(
        flowcellBarcode = barcode,
        lane = lane,
        libraryName = "library" + randomId,
        location = Location.GCP
      )
      val upsertMetadata = UbamMetadata(
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
        results <- runClientGetJsonAs[Seq[Json]](
          ClioCommand.queryWgsUbamName,
          "--project",
          project,
          "--flowcell-barcode",
          barcode
        )
      } yield {
        results.length should be(expectedLength)
        results.foreach { result =>
          result.unsafeGet[String]("project") should be(project)
          result.unsafeGet[String]("sample_alias") should be(sample)
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
      _ <- runUpsertUbam(
        deleteKey,
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted)),
        SequencingType.WholeGenome
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runClientGetJsonAs[Seq[Json]](
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
        result.unsafeGet[String]("project") should be(project)
        result.unsafeGet[String]("sample_alias") should be(sample)

        val resultKey = UbamKey(
          flowcellBarcode = result.unsafeGet[String]("flowcell_barcode"),
          lane = result.unsafeGet[Int]("lane"),
          libraryName = result.unsafeGet[String]("library_name"),
          location = result.unsafeGet[Location]("location")
        )

        result.unsafeGet[DocumentStatus]("document_status") should be {
          if (resultKey == deleteKey) DocumentStatus.Deleted else DocumentStatus.Normal
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
    val sourceDir = rootTestStorageDir / s"wgs-ubam/$barcode/$lane/$library/$ubamName/"
    val targetDir = if (srcIsDest) {
      sourceDir
    } else {
      sourceDir.parent / "moved/"
    }
    val sourceUbam = sourceDir / ubamName
    val targetUbam = targetDir / ubamName

    val key = UbamKey(Location.GCP, barcode, lane, library)
    val metadata = UbamMetadata(ubamPath = Some(sourceUbam.uri))

    // Clio needs the metadata to be added before it can be moved.
    val _ = sourceUbam.write(fileContents)
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
        targetDir.uri.toString
      )
    } yield {
      if (!srcIsDest) {
        sourceUbam shouldNot exist
      }
      targetUbam should exist
      targetUbam.contentAsString should be(fileContents)
    }

    result.andThen[Unit] {
      case _ => {
        // Without `val _ =`, the compiler complains about discarded non-Unit value.
        val _ = Seq(sourceUbam, targetUbam).map(_.delete(swallowIOExceptions = true))
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

    val cloudPath = rootTestStorageDir / s"wgs-ubam/$barcode/$lane/$library/$randomId${UbamExtensions.UbamExtension}"
    val cloudPath2 = cloudPath.parent / s"moved/$randomId${UbamExtensions.UbamExtension}"

    val key = UbamKey(Location.GCP, barcode, lane, library)
    val metadata = UbamMetadata(ubamPath = Some(cloudPath.uri))

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
          cloudPath2.uri.toString
        )
      }
      queryOutputs <- runClientGetJsonAs[Seq[Json]](
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
      cloudPath2 shouldNot exist
      queryOutputs should have length 1
      queryOutputs.head.unsafeGet[URI]("ubam_path") should be(cloudPath.uri)
    }

    result.andThen[Unit] {
      case _ => {
        val _ = cloudPath2.delete(swallowIOExceptions = true)
      }
    }
  }

  it should "respect user-set regulatory designation for wgs ubams" in {
    val flowcellBarcode = s"testRegulatoryDesignation.$randomId"
    val library = s"library.$randomId"
    val lane = 1
    val regulatoryDesignation = RegulatoryDesignation.ClinicalDiagnostics
    val upsertKey = UbamKey(Location.GCP, flowcellBarcode, lane, library)
    val metadata = UbamMetadata(
      project = Some("testProject1"),
      regulatoryDesignation = Some(regulatoryDesignation)
    )

    def query = {
      for {
        results <- runClientGetJsonAs[Seq[Json]](
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
      queried.unsafeGet[RegulatoryDesignation]("regulatory_designation") should be(
        regulatoryDesignation
      )
    }
  }

  def testDeleteUbam(
    existingNote: Option[String] = None,
    testNonExistingFile: Boolean = false,
    force: Boolean = false
  ): Future[Assertion] = {
    val deleteNote = s"$randomId --- Deleted by the integration tests --- $randomId"

    val barcode = s"barcode$randomId"
    val lane = 4
    val library = s"lib$randomId"

    val fileContents = s"$randomId --- I am fated to die --- $randomId"
    val cloudPath = rootTestStorageDir / s"wgs-ubam/$barcode/$lane/$library/$randomId${UbamExtensions.UbamExtension}"

    val key = UbamKey(Location.GCP, barcode, lane, library)
    val metadata =
      UbamMetadata(notes = existingNote, ubamPath = Some(cloudPath.uri))

    // Clio needs the metadata to be added before it can be deleted.
    val _ = if (!testNonExistingFile) {
      cloudPath.write(fileContents)
    }

    val result = for {
      _ <- runUpsertUbam(key, metadata, SequencingType.WholeGenome)
      _ <- runClient(
        ClioCommand.deleteWgsUbamName,
        Seq(
          "--flowcell-barcode",
          barcode,
          "--lane",
          lane.toString,
          "--library-name",
          library,
          "--location",
          Location.GCP.entryName,
          "--note",
          deleteNote,
          if (force) "--force" else ""
        ).filter(_.nonEmpty): _*
      )
      _ = cloudPath shouldNot exist
      outputs <- runClientGetJsonAs[Seq[Json]](
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
        val _ = cloudPath.delete(swallowIOExceptions = true)
      }
    }
  }

  it should "delete wgs-ubams in GCP" in testDeleteUbam()

  it should "preserve existing notes when deleting wgs-ubams" in testDeleteUbam(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  it should "throw an exception when trying to delete a ubam if a file does not exist" in {
    recoverToSucceededIf[Exception] {
      testDeleteUbam(testNonExistingFile = true)
    }
  }

  it should "delete a ubam if a file does not exist and force is true" in testDeleteUbam(
    testNonExistingFile = true,
    force = true
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

  it should "upsert a new ubam if force is false" in {
    val upsertKey = UbamKey(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )

    for {
      upsertId1 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(project = Some("testProject1")),
        SequencingType.WholeGenome,
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsUbam)
      storedDocument1.unsafeGet[String]("project") should be("testProject1")
    }
  }

  it should "allow an upsert that modifies values not already set or are unchanged if force is false" in {
    val upsertKey = UbamKey(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )

    for {
      upsertId1 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(project = Some("testProject1")),
        SequencingType.WholeGenome,
        force = false
      )
      upsertId2 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(
          project = Some("testProject1"),
          sampleAlias = Some("sampleAlias1")
        ),
        SequencingType.WholeGenome,
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsUbam)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.WgsUbam)
      storedDocument1.unsafeGet[String]("project") should be("testProject1")
      storedDocument2.unsafeGet[String]("sample_alias") should be("sampleAlias1")
    }
  }

  it should "not allow an upsert that modifies values already set if force is false" in {
    val upsertKey = UbamKey(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )
    for {
      upsertId1 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(project = Some("testProject1")),
        SequencingType.WholeGenome,
        force = false
      )
      _ <- recoverToSucceededIf[Exception] {
        runUpsertUbam(
          upsertKey,
          UbamMetadata(project = Some("testProject2")),
          SequencingType.WholeGenome,
          force = false
        )
      }
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.WgsUbam)
      storedDocument1.unsafeGet[String]("project") should be("testProject1")
    }
  }
}
