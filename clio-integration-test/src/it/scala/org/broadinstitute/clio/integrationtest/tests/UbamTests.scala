package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import io.circe.syntax._
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json
import org.broadinstitute.clio.client.commands.ClioCommand
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
  import org.broadinstitute.clio.JsonUtils.JsonOps

  def runUpsertUbam(
    key: UbamKey,
    metadata: UbamMetadata,
    // TODO: Rip this param out when finished phasing out wgs command usage in other codebases
    useOldCommands: Boolean = false,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    val command =
      if (useOldCommands) ClioCommand.addWgsUbamName
      else ClioCommand.addUbamName
    runDecode[UpsertId](
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
    )
  }

  it should "create the expected ubam mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import ElasticsearchUtil.HttpClientOps

    val expected = ElasticsearchIndex.Ubam
    val getRequest = getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.executeAndUnpack(getRequest).map {
      _ should be(Seq(indexToMapping(expected)))
    }
  }

  // Generate a test for every combination of Location value and command type.
  for {
    location <- Location.values
    useOldCommands <- Seq(true, false)
  } yield it should behave like testUbamLocation(location, useOldCommands)

  /**
    * Utility method for generating an assertion about behavior for a Location key.
    *
    * @see http://www.scalatest.org/user_guide/sharing_tests
    */
  def testUbamLocation(location: Location, useOldCommands: Boolean): Unit = {
    val testNameAddition =
      if (useOldCommands) " using the old wgs commands"
      else ""
    it should s"handle upserts and queries for ubam location $location$testNameAddition" in {
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
      val queryCommand =
        if (useOldCommands) ClioCommand.queryWgsUbamName
        else ClioCommand.queryUbamName

      for {
        returnedUpsertId <- runUpsertUbam(
          key,
          metadata.copy(documentStatus = None),
          useOldCommands = useOldCommands
        )
        queryResponse <- runCollectJson(
          queryCommand,
          "--library-name",
          key.libraryName
        )
      } yield {
        queryResponse should contain only expected
        val storedDocument = getJsonFrom(returnedUpsertId)(ElasticsearchIndex.Ubam)
        storedDocument.mapObject(
          _.filterKeys(!ElasticsearchIndex.BookkeepingNames.contains(_))
        ) should be(expected)
      }
    }
  }

  it should "assign different upsertIds to different ubam upserts" in {
    val upsertKey = UbamKey(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )

    for {
      upsertId1 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(project = Some("testProject1"))
      )
      upsertId2 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(project = Some("testProject2"))
      )
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Ubam)
      storedDocument1.unsafeGet[String]("project") should be("testProject1")

      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Ubam)
      storedDocument2.unsafeGet[String]("project") should be("testProject2")

      storedDocument1.deepMerge {
        Json.obj(
          ElasticsearchIndex.UpsertIdElasticsearchName -> upsertId2.asJson,
          "project" -> "testProject2".asJson
        )
      } should be(storedDocument2)
    }
  }

  it should "assign different upsertIds to equal ubam upserts" in {
    val upsertKey = UbamKey(
      Location.GCP,
      "testupsertIdBarcode",
      2,
      s"library$randomId"
    )
    val metadata = UbamMetadata(project = Some("testProject1"))

    for {
      upsertId1 <- runUpsertUbam(upsertKey, metadata)
      upsertId2 <- runUpsertUbam(upsertKey, metadata)
    } yield {
      upsertId2 should be > upsertId1

      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Ubam)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Ubam)

      storedDocument1.mapObject(
        _.add(ElasticsearchIndex.UpsertIdElasticsearchName, upsertId2.asJson)
      ) should be(storedDocument2)
    }
  }

  it should "handle querying ubams by sample, project, and research-project-id" in {
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
          runUpsertUbam(key, metadata)
      }
    }

    for {
      _ <- upserts
      projectResults <- runCollectJson(
        ClioCommand.queryUbamName,
        "--project",
        project
      )
      sampleResults <- runCollectJson(
        ClioCommand.queryUbamName,
        "--sample-alias",
        samples.head
      )
      rpIdResults <- runCollectJson(
        ClioCommand.queryUbamName,
        "--research-project-id",
        researchProjectIds.last
      )
    } yield {
      projectResults should have length 3
      projectResults.foreach { result =>
        result.unsafeGet[String]("project") should be(project)
      }
      sampleResults should have length 2
      sampleResults.foreach { result =>
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
    val project = s"testProject$randomId"

    val samples = Seq.fill(3)(s"sample$randomId")

    val researchProjectIds = Seq.fill(3)(s"rpId$randomId")

    val aggregations = Seq(
      AggregatedBy.Squid,
      AggregatedBy.Squid,
      AggregatedBy.RP
    )

    val upserts = Future.sequence {
      samples zip researchProjectIds zip aggregations map {
        case ((samp, rpid), agg) => (samp, rpid, agg)
      } map {
        case (sample, researchProjectId, aggregation) =>
          val key = UbamKey(location, flowcellBarcode, lane, s"library$randomId")
          val metadata = UbamMetadata(
            project = Some(project),
            sampleAlias = Some(sample),
            researchProjectId = Some(researchProjectId),
            aggregatedBy = Some(aggregation)
          )
          runUpsertUbam(key, metadata)
      }
    }

    for {
      _ <- upserts
      queryResults <- runCollectJson(
        ClioCommand.queryUbamName,
        "--project",
        project,
        "--aggregated-by",
        AggregatedBy.Squid.entryName
      )
    } yield {
      queryResults should have length 2
      queryResults.foreach { result =>
        result.unsafeGet[String]("project") should be(project)
      }
      queryResults.foreach { result =>
        researchProjectIds.take(2) should contain(
          result.unsafeGet[String]("research_project_id")
        )
      }
      succeed
    }
  }

  it should "handle updates to ubam metadata" in {
    val key = UbamKey(Location.GCP, "barcode2", 2, s"library$randomId")
    val project = s"testProject$randomId"
    val metadata = UbamMetadata(
      project = Some(project),
      sampleAlias = Some("sampleAlias1"),
      notes = Some("Breaking news")
    )

    def query = {
      for {
        results <- runCollectJson(
          ClioCommand.queryUbamName,
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
      _ <- runUpsertUbam(key, upsertData)
      original <- query
      _ = original.unsafeGet[String]("sample_alias") should be("sampleAlias1")
      _ = original.unsafeGet[Option[String]]("notes") should be(None)

      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertUbam(key, upsertData2)
      withNotes <- query
      _ = withNotes.unsafeGet[String]("sample_alias") should be("sampleAlias1")
      _ = withNotes.unsafeGet[String]("notes") should be("Breaking news")

      _ <- runUpsertUbam(
        key,
        upsertData2.copy(sampleAlias = Some("sampleAlias2"), notes = Some(""))
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
        case (key, metadata) => runUpsertUbam(key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        results <- runCollectJson(
          ClioCommand.queryUbamName,
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
        deleteData.copy(documentStatus = Some(DocumentStatus.Deleted))
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runCollectJson(
        ClioCommand.queryUbamName,
        "--project",
        project,
        "--flowcell-barcode",
        barcode,
        "--include-deleted"
      )
    } yield {
      results.length should be(keysWithMetadata.length)
      results.foreach { result =>
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
      succeed
    }
  }

  def testMoveUbam(
    srcIsDest: Boolean = false,
    // TODO: Rip this param out when finished phasing out wgs command usage in other codebases
    useOldCommand: Boolean = false
  ): Future[Assertion] = {
    val barcode = s"barcode$randomId"
    val lane = 3
    val library = s"lib$randomId"

    val fileContents = s"$randomId --- I am a dummy ubam --- $randomId"
    val ubamName = s"$randomId${UbamExtensions.UbamExtension}"
    val sourceDir = rootTestStorageDir / s"ubam/$barcode/$lane/$library/$ubamName/"
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
    val moveCommand =
      if (useOldCommand) ClioCommand.moveWgsUbamName
      else ClioCommand.moveUbamName
    val result = for {
      _ <- runUpsertUbam(key, metadata)
      _ <- runIgnore(
        moveCommand,
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

  it should "move ubams in GCP" in testMoveUbam()

  it should "move ubams using the old wgs command" in testMoveUbam(useOldCommand = true)

  it should "not delete ubams when moving and source == destination" in testMoveUbam(
    srcIsDest = true
  )

  it should "not move ubams without a destination" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.moveUbamName,
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

  it should "not move ubams when the source file doesn't exist" in {
    val barcode = s"barcode$randomId"
    val lane = 3
    val library = s"lib$randomId"

    val cloudPath = rootTestStorageDir / s"ubam/$barcode/$lane/$library/$randomId${UbamExtensions.UbamExtension}"
    val cloudPath2 = cloudPath.parent / s"moved/$randomId${UbamExtensions.UbamExtension}"

    val key = UbamKey(Location.GCP, barcode, lane, library)
    val metadata = UbamMetadata(ubamPath = Some(cloudPath.uri))

    val result = for {
      _ <- runUpsertUbam(key, metadata)
      _ <- recoverToSucceededIf[Exception] {
        runDecode[UpsertId](
          ClioCommand.moveUbamName,
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
      queryOutputs <- runCollectJson(
        ClioCommand.queryUbamName,
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

  it should "respect user-set regulatory designation for ubams" in {
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
        results <- runCollectJson(
          ClioCommand.queryUbamName,
          "--flowcell-barcode",
          flowcellBarcode
        )
      } yield {
        results should have length 1
        results.head
      }
    }
    for {
      upsert <- runUpsertUbam(upsertKey, metadata)
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
    force: Boolean = false,
    // TODO: Rip this param out when finished phasing out wgs command usage in other codebases
    useOldCommand: Boolean = false
  ): Future[Assertion] = {
    val deleteNote = s"$randomId --- Deleted by the integration tests --- $randomId"

    val barcode = s"barcode$randomId"
    val lane = 4
    val library = s"lib$randomId"

    val fileContents = s"$randomId --- I am fated to die --- $randomId"
    val cloudPath = rootTestStorageDir / s"ubam/$barcode/$lane/$library/$randomId${UbamExtensions.UbamExtension}"

    val key = UbamKey(Location.GCP, barcode, lane, library)
    val metadata =
      UbamMetadata(notes = existingNote, ubamPath = Some(cloudPath.uri))

    // Clio needs the metadata to be added before it can be deleted.
    val _ = if (!testNonExistingFile) {
      cloudPath.write(fileContents)
    }
    val deleteCommand =
      if (useOldCommand) ClioCommand.deleteWgsUbamName
      else ClioCommand.deleteUbamName

    val result = for {
      _ <- runUpsertUbam(key, metadata)
      _ <- runIgnore(
        deleteCommand,
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
      outputs <- runCollectJson(
        ClioCommand.queryUbamName,
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

  it should "delete ubams in GCP" in testDeleteUbam()

  it should "delete ubams using the old wgs commands" in testDeleteUbam(
    useOldCommand = true
  )

  it should "preserve existing notes when deleting ubams" in testDeleteUbam(
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

  it should "not delete ubams without a note" in {
    recoverToExceptionIf[Exception] {
      runDecode[UpsertId](
        ClioCommand.deleteUbamName,
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
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Ubam)
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
        force = false
      )
      upsertId2 <- runUpsertUbam(
        upsertKey,
        UbamMetadata(
          project = Some("testProject1"),
          sampleAlias = Some("sampleAlias1")
        ),
        force = false
      )
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Ubam)
      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Ubam)
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
        force = false
      )
      _ <- recoverToSucceededIf[Exception] {
        runUpsertUbam(
          upsertKey,
          UbamMetadata(project = Some("testProject2")),
          force = false
        )
      }
    } yield {
      val storedDocument1 = getJsonFrom(upsertId1)(ElasticsearchIndex.Ubam)
      storedDocument1.unsafeGet[String]("project") should be("testProject1")
    }
  }
}
