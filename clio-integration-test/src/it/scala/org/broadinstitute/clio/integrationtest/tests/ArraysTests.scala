package org.broadinstitute.clio.integrationtest.tests

import java.net.URI
import java.util.UUID

import better.files.File
import com.sksamuel.elastic4s.IndexAndType
import io.circe.Json
import io.circe.literal._
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
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation,
  UpsertId
}
import org.scalatest.Assertion

import scala.concurrent.Future

trait ArraysTests { self: BaseIntegrationSpec =>
  import org.broadinstitute.clio.JsonUtils.JsonOps

  def runUpsertArrays(
    key: ArraysKey,
    metadata: ArraysMetadata,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runDecode[UpsertId](
      ClioCommand.addArraysName,
      Seq(
        "--location",
        key.location.entryName,
        "--chipwell-barcode",
        key.chipwellBarcode.name,
        "--version",
        key.version.toString,
        "--metadata-location",
        tmpMetadata.toString,
        if (force) "--force" else ""
      ).filter(_.nonEmpty): _*
    )
  }

  it should "return JSON [] when nothing matches query" in {
    for {
      result <- runCollectJson(
        ClioCommand.queryArraysName,
        "--project",
        UUID.randomUUID.toString
      )
    } yield {
      result.asJson shouldEqual json"[]"
    }
  }

  it should "create the expected arrays mapping in elasticsearch" in {
    import ElasticsearchUtil.HttpClientOps
    import com.sksamuel.elastic4s.http.ElasticDsl._

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
        chipwellBarcode = Symbol(s"barcode$randomId"),
        version = 2
      )
      val metadata = ArraysMetadata(
        documentStatus = Some(DocumentStatus.Normal),
        chipType = Some("test chipType")
      )
      val expected = expectedMerge(key, metadata)

      for {
        returnedUpsertId <- runUpsertArrays(key, metadata.copy(documentStatus = None))
        outputs <- runCollectJson(
          ClioCommand.queryArraysName,
          "--chipwell-barcode",
          key.chipwellBarcode.name
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
      chipwellBarcode = Symbol(s"barcode$randomId"),
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
      storedDocument1.unsafeGet[String]("chip_type") should be("test chipType")

      val storedDocument2 = getJsonFrom(upsertId2)(ElasticsearchIndex.Arrays)
      storedDocument2.unsafeGet[String]("chip_type") should be("test chipType2")

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
      chipwellBarcode = Symbol(s"barcode$randomId"),
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

  it should "handle querying arrays by barcode" in {
    val location = Location.GCP

    val barcodes = {
      val sameId = s"barcode$randomId"
      Seq(sameId, sameId, s"barcode$randomId")
    }

    val upserts = Future.sequence {
      barcodes.zip(1 to 3).map {
        case (barcode, version) =>
          val key = ArraysKey(location, Symbol(barcode), version)
          val data = ArraysMetadata(
            chipType = Some("chip type"),
            isZcalled = Some(true)
          )
          runUpsertArrays(key, data)
      }
    }

    for {
      _ <- upserts
      barcodeResults <- runCollectJson(
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        barcodes.head
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
      prefixResults <- runCollectJson(
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        prefix
      )
      suffixResults <- runCollectJson(
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
        results <- runCollectJson(
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
      _ = original.unsafeGet[String]("chip_type") should be(chipType)
      _ = original.unsafeGet[Boolean]("is_zcalled") should be(isZcalled)
      _ = original.unsafeGet[Option[String]]("notes") should be(None)

      upsertData2 = upsertData.copy(notes = metadata.notes)
      _ <- runUpsertArrays(key, upsertData2)
      withNotes <- query
      _ = withNotes.unsafeGet[String]("chip_type") should be(chipType)
      _ = withNotes.unsafeGet[Boolean]("is_zcalled") should be(isZcalled)
      _ = withNotes.unsafeGet[String]("notes") should be(initialNotes)

      _ <- runUpsertArrays(
        key,
        upsertData2.copy(isZcalled = Some(false), notes = Some(""))
      )
      emptyNotes <- query
    } yield {
      emptyNotes.unsafeGet[String]("chip_type") should be(chipType)
      emptyNotes.unsafeGet[Boolean]("is_zcalled") should be(false)
      emptyNotes.unsafeGet[String]("notes") should be("")
    }
  }

  def testQueryAll(documentStatus: DocumentStatus): Future[Assertion] = {
    val queryArg = documentStatus match {
      case DocumentStatus.Deleted  => "--include-deleted"
      case DocumentStatus.External => "--include-all"
      case _                       => ""
    }
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
    val (notNormalKey, notNormalData) = keysWithMetadata.head

    val upserts = Future.sequence {
      keysWithMetadata.map {
        case (v1Key, metadata) => runUpsertArrays(v1Key, metadata)
      }
    }

    def checkQuery(expectedLength: Int) = {
      for {
        results <- runCollectJson(
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
        notNormalKey,
        notNormalData.copy(documentStatus = Some(documentStatus))
      )
      _ <- checkQuery(expectedLength = 2)

      results <- runCollectJson(
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        barcode,
        queryArg
      )
    } yield {
      results.length should be(keysWithMetadata.length)
      results.foldLeft(succeed) { (_, result) =>
        result.unsafeGet[String]("chipwell_barcode") should be(barcode)

        val resultKey = ArraysKey(
          location = result.unsafeGet[Location]("location"),
          chipwellBarcode = result.unsafeGet[Symbol]("chipwell_barcode"),
          version = result.unsafeGet[Int]("version")
        )

        result.unsafeGet[DocumentStatus]("document_status") should be {
          if (resultKey == notNormalKey) documentStatus else DocumentStatus.Normal
        }
      }
    }
  }

  it should "show deleted arrays records on queryAll, but not query" in {
    testQueryAll(DocumentStatus.Deleted)
  }

  it should "show External arrays records on queryAll, but not query" in {
    testQueryAll(DocumentStatus.External)
  }

  it should "respect user-set regulatory designation for arrays" in {
    val chipwellBarcode = s"barcode$randomId"
    val regulatoryDesignation = RegulatoryDesignation.ClinicalDiagnostics
    val upsertKey = ArraysKey(Location.GCP, Symbol(chipwellBarcode), 1)
    val metadata = ArraysMetadata(
      regulatoryDesignation = Some(regulatoryDesignation)
    )

    def query = {
      for {
        results <- runCollectJson(
          ClioCommand.queryArraysName,
          "--chipwell-barcode",
          chipwellBarcode
        )
      } yield {
        results should have length 1
        results.head
      }
    }
    for {
      upsert <- runUpsertArrays(upsertKey, metadata)
      queried <- query
    } yield {
      queried.unsafeGet[RegulatoryDesignation]("regulatory_designation") should be(
        regulatoryDesignation
      )
    }
  }

  it should "not overwrite existing regulatory designation on arrays delivery" in {
    val id = randomId
    val chipwellBarcode = s"barcode$randomId"
    val version = 1
    val regulatoryDesignation = RegulatoryDesignation.ClinicalDiagnostics

    val vcfContents = s"$id --- I am a dummy vcf --- $id"
    val vcfIndexContents = s"$id --- I am a dummy vcfIndex --- $id"
    val gtcContents = s"$id --- I am a dummy gtc --- $id"
    val grnIdatContents = s"$id --- I am a dummy grn idat --- $id"
    val redIdatContents = s"$id --- I am a dummy red idat --- $id"

    val vcfName = s"$chipwellBarcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$chipwellBarcode${ArraysExtensions.VcfGzTbiExtension}"
    val gtcName = s"$chipwellBarcode${ArraysExtensions.GtcExtension}"
    val grnIdatName = s"grn-$id${ArraysExtensions.GrnIdatExtension}"
    val redIdatName = s"red-$id${ArraysExtensions.RedIdatExtension}"

    val rootSource = rootTestStorageDir / s"arrays/$chipwellBarcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName
    val gtcSource = rootSource / gtcName
    val grnIdatSource = rootSource / "idats" / grnIdatName
    val redIdatSource = rootSource / "idats" / redIdatName

    val key = ArraysKey(Location.GCP, Symbol(chipwellBarcode), 1)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      gtcPath = Some(gtcSource.uri),
      grnIdatPath = Some(grnIdatSource.uri),
      redIdatPath = Some(redIdatSource.uri),
      regulatoryDesignation = Some(regulatoryDesignation),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    val _ = Seq(
      (vcfSource, vcfContents),
      (vcfIndexSource, vcfIndexContents),
      (gtcSource, gtcContents),
      (grnIdatSource, grnIdatContents),
      (redIdatSource, redIdatContents)
    ).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runIgnore(
        ClioCommand.deliverArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        chipwellBarcode,
        "--version",
        version.toString,
        "--workspace-name",
        workspaceName,
        "--workspace-path",
        rootSource.uri.toString
      )
      outputs <- runCollectJson(
        ClioCommand.queryArraysName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(vcfSource, vcfIndexSource, gtcSource).foreach(_ should exist)

      Seq(
        (vcfSource, vcfContents),
        (vcfIndexSource, vcfIndexContents),
        (gtcSource, gtcContents),
        (grnIdatSource, grnIdatContents),
        (redIdatSource, redIdatContents)
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
      case _ =>
        val _ = Seq(vcfSource, vcfIndexSource)
          .map(_.delete(swallowIOExceptions = true))
    }
  }

  def testMove(changeBasename: Boolean = false): Future[Assertion] = {
    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfContents = s"$id --- I am a dummy vcf --- $id"
    val vcfIndexContents = s"$id --- I am a dummy vcfIndex --- $id"
    val gtcContents = s"$id --- I am a dummy gtc --- $id"
    val paramsContents = s"$id --- I am a dummy params file --- $id"
    val fingerprintingDetailMetricsContents =
      s"$id --- I am dummy fingerprintingDetail metrics --- $id"
    val fingerprintingSummaryMetricsContents =
      s"$id --- I am dummy fingerprintingSummary metrics --- $id"
    val variantCallingSummaryMetricsContents =
      s"$id --- I am dummy variant_calling_summary_metrics --- $id"
    val variantCallingDetailMetricsContents =
      s"$id --- I am dummy variant_calling_detail_metrics --- $id"
    val genotypeConcordanceSummaryMetricsContents =
      s"$id --- I am dummy genotype_concordance_summary_metrics --- $id"
    val genotypeConcordanceDetailMetricsContents =
      s"$id --- I am dummy genotype_concordance_detail_metrics --- $id"
    val genotypeConcordanceContingencyMetricsContents =
      s"$id --- I am dummy genotype_concordance_contingency_metrics --- $id"

    val rootSource = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfSource = rootSource / s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexSource = rootSource / s"$barcode${ArraysExtensions.VcfGzTbiExtension}"
    val gtcSource = rootSource / s"$barcode${ArraysExtensions.GtcExtension}"
    val paramsSource = rootSource / s"params-$id${ArraysExtensions.TxtExtension}"
    val fingerprintingSummaryMetricsSource = rootSource / s"$barcode${ArraysExtensions.FingerprintingSummaryMetricsExtension}"
    val fingerprintingDetailMetricsSource = rootSource / s"$barcode${ArraysExtensions.FingerprintingDetailMetricsExtension}"
    val variantCallingSummaryMetricsSource = rootSource / s"$barcode${ArraysExtensions.VariantCallingSummaryMetricsExtension}"
    val variantCallingDetailMetricsSource = rootSource / s"$barcode${ArraysExtensions.VariantCallingDetailMetricsExtension}"
    val genotypeConcordanceSummaryMetricsSource = rootSource / s"$barcode${ArraysExtensions.GenotypeConcordanceSummaryMetricsExtension}"
    val genotypeConcordanceDetailMetricsSource = rootSource / s"$barcode${ArraysExtensions.GenotypeConcordanceDetailMetricsExtension}"
    val genotypeConcordanceContingencyMetricsSource = rootSource / s"$barcode${ArraysExtensions.GenotypeConcordanceContingencyMetricsExtension}"

    val endBasename = if (changeBasename) randomId else barcode

    val rootDestination = rootSource.parent / s"moved/$id/"
    val vcfDestination = rootDestination / s"$endBasename${ArraysExtensions.VcfGzExtension}"
    val vcfIndexDestination = rootDestination / s"$endBasename${ArraysExtensions.VcfGzTbiExtension}"
    val gtcDestination = rootDestination / s"$endBasename${ArraysExtensions.GtcExtension}"
    val paramsDestination = rootDestination / s"params-$id${ArraysExtensions.TxtExtension}"
    val fingerprintSummaryMetricsDestination = rootDestination / s"$endBasename${ArraysExtensions.FingerprintingSummaryMetricsExtension}"
    val fingerprintDetailMetricsDestination = rootDestination / s"$endBasename${ArraysExtensions.FingerprintingDetailMetricsExtension}"
    val variantCallingSummaryMetricsDestination = rootDestination / s"$endBasename${ArraysExtensions.VariantCallingSummaryMetricsExtension}"
    val variantCallingDetailMetricsDestination = rootDestination / s"$endBasename${ArraysExtensions.VariantCallingDetailMetricsExtension}"
    val genotypeConcordanceSummaryMetricsDestination = rootDestination / s"$endBasename${ArraysExtensions.GenotypeConcordanceSummaryMetricsExtension}"
    val genotypeConcordanceDetailMetricsDestination = rootDestination / s"$endBasename${ArraysExtensions.GenotypeConcordanceDetailMetricsExtension}"
    val genotypeConcordanceContingencyMetricsDestination = rootDestination / s"$endBasename${ArraysExtensions.GenotypeConcordanceContingencyMetricsExtension}"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      gtcPath = Some(gtcSource.uri),
      paramsPath = Some(paramsSource.uri),
      fingerprintingSummaryMetricsPath = Some(fingerprintingSummaryMetricsSource.uri),
      fingerprintingDetailMetricsPath = Some(fingerprintingDetailMetricsSource.uri),
      variantCallingSummaryMetricsPath = Some(variantCallingSummaryMetricsSource.uri),
      variantCallingDetailMetricsPath = Some(variantCallingDetailMetricsSource.uri),
      genotypeConcordanceSummaryMetricsPath =
        Some(genotypeConcordanceSummaryMetricsSource.uri),
      genotypeConcordanceDetailMetricsPath =
        Some(genotypeConcordanceDetailMetricsSource.uri),
      genotypeConcordanceContingencyMetricsPath =
        Some(genotypeConcordanceContingencyMetricsSource.uri)
    )

    val _ = Seq(
      (vcfSource, vcfContents),
      (vcfIndexSource, vcfIndexContents),
      (gtcSource, gtcContents),
      (paramsSource, paramsContents),
      (fingerprintingSummaryMetricsSource, fingerprintingSummaryMetricsContents),
      (fingerprintingDetailMetricsSource, fingerprintingDetailMetricsContents),
      (variantCallingSummaryMetricsSource, variantCallingSummaryMetricsContents),
      (variantCallingDetailMetricsSource, variantCallingDetailMetricsContents),
      (
        genotypeConcordanceSummaryMetricsSource,
        genotypeConcordanceSummaryMetricsContents
      ),
      (genotypeConcordanceDetailMetricsSource, genotypeConcordanceDetailMetricsContents),
      (
        genotypeConcordanceContingencyMetricsSource,
        genotypeConcordanceContingencyMetricsContents
      )
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
      _ <- runIgnore(ClioCommand.moveArraysName, args: _*)
    } yield {
      Seq(
        vcfSource,
        vcfIndexSource,
        gtcSource,
        paramsSource,
        fingerprintingSummaryMetricsSource,
        fingerprintingDetailMetricsSource,
        variantCallingSummaryMetricsSource,
        variantCallingDetailMetricsSource,
        genotypeConcordanceSummaryMetricsSource,
        genotypeConcordanceDetailMetricsSource,
        genotypeConcordanceContingencyMetricsSource
      ).foreach(_ shouldNot exist)

      Seq(
        vcfDestination,
        vcfIndexDestination,
        gtcDestination,
        paramsDestination,
        fingerprintSummaryMetricsDestination,
        fingerprintDetailMetricsDestination,
        variantCallingSummaryMetricsDestination,
        variantCallingDetailMetricsDestination,
        genotypeConcordanceSummaryMetricsDestination,
        genotypeConcordanceDetailMetricsDestination,
        genotypeConcordanceContingencyMetricsDestination
      ).foreach(_ should exist)

      Seq(
        (vcfDestination, vcfContents),
        (vcfIndexDestination, vcfIndexContents),
        (gtcDestination, gtcContents),
        (paramsDestination, paramsContents),
        (fingerprintSummaryMetricsDestination, fingerprintingSummaryMetricsContents),
        (fingerprintDetailMetricsDestination, fingerprintingDetailMetricsContents),
        (variantCallingSummaryMetricsDestination, variantCallingSummaryMetricsContents),
        (variantCallingDetailMetricsDestination, variantCallingDetailMetricsContents),
        (
          genotypeConcordanceSummaryMetricsDestination,
          genotypeConcordanceSummaryMetricsContents
        ),
        (
          genotypeConcordanceDetailMetricsDestination,
          genotypeConcordanceDetailMetricsContents
        ),
        (
          genotypeConcordanceContingencyMetricsDestination,
          genotypeConcordanceContingencyMetricsContents
        )
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
          vcfIndexDestination,
          gtcSource,
          gtcDestination,
          paramsSource,
          paramsDestination,
          fingerprintingSummaryMetricsSource,
          fingerprintSummaryMetricsDestination,
          fingerprintingDetailMetricsSource,
          fingerprintDetailMetricsDestination,
          variantCallingSummaryMetricsSource,
          variantCallingSummaryMetricsDestination,
          variantCallingDetailMetricsSource,
          variantCallingDetailMetricsDestination,
          genotypeConcordanceSummaryMetricsSource,
          genotypeConcordanceSummaryMetricsDestination,
          genotypeConcordanceDetailMetricsSource,
          genotypeConcordanceDetailMetricsDestination,
          genotypeConcordanceContingencyMetricsSource,
          genotypeConcordanceContingencyMetricsDestination
        ).map(_.delete(swallowIOExceptions = true))
    }
  }

  it should "move the vcf and vcfIndex together in GCP" in testMove()

  it should "support changing the vcf and vcfIndex basename on move" in testMove(
    changeBasename = true
  )

  it should "not move arrays without a destination" in {
    recoverToExceptionIf[Exception] {
      runIgnore(
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
        runIgnore(
          ClioCommand.moveArraysName,
          "--location",
          key.location.entryName,
          "--chipwell-barcode",
          key.chipwellBarcode.name,
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

  def testExternalArrays(
    existingNote: Option[String] = None
  ): Future[Assertion] = {
    val markExternalNote =
      s"$randomId --- Marked External by the integration tests --- $randomId"

    val chipwellBarcode = s" chip$randomId"
    val version = 3

    val vcfContents = s"$randomId --- I am a vcf fated for other worlds --- $randomId"
    val vcfIndexContents =
      s"$randomId --- I am an index fated for other worlds --- $randomId"
    val metrics1Contents = s"$randomId --- I am a questing metrics file --- $randomId"
    val metrics2Contents =
      s"$randomId --- I am a second questing metrics file --- $randomId"

    val storageDir = rootTestStorageDir / s"arrays/$chipwellBarcode/$version/"
    val vcfPath = storageDir / s"$randomId${ArraysExtensions.VcfGzExtension}"
    val vcfIndexPath = storageDir / s"$randomId${ArraysExtensions.VcfGzTbiExtension}"
    val metrics1Path = storageDir / s"$randomId.metrics"
    val metrics2Path = storageDir / s"$randomId.metrics"

    val key = ArraysKey(Location.GCP, Symbol(chipwellBarcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfPath.uri),
      vcfIndexPath = Some(vcfIndexPath.uri),
      variantCallingDetailMetricsPath = Some(metrics1Path.uri),
      variantCallingSummaryMetricsPath = Some(metrics1Path.uri),
      notes = existingNote
    )

    Seq(
      (vcfPath, vcfContents),
      (vcfIndexPath, vcfIndexContents),
      (metrics1Path, metrics1Contents),
      (metrics2Path, metrics2Contents)
    ).map {
      case (path, contents) => path.write(contents)
    }

    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runIgnore(
        ClioCommand.markExternalArraysName,
        Seq(
          "--location",
          Location.GCP.entryName,
          "--chipwell-barcode",
          chipwellBarcode,
          "--version",
          version.toString,
          "--note",
          markExternalNote
        ).filter(_.nonEmpty): _*
      )
      outputs <- runCollectJson(
        ClioCommand.queryArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        chipwellBarcode,
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
        val _ = Seq(vcfPath, vcfIndexPath, metrics1Path, metrics2Path)
          .map(_.delete(swallowIOExceptions = true))
      }
    }
  }

  it should "mark arrays as External when marking external" in {
    testExternalArrays()
  }

  it should "require a note when marking arrays as External" in {
    recoverToExceptionIf[Exception] {
      runIgnore(
        ClioCommand.markExternalArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        randomId,
        "--version",
        "123"
      )
    }.map {
      _.getMessage should include("--note")
    }
  }

  it should "preserve existing notes when marking arrays as External" in testExternalArrays(
    existingNote = Some(s"$randomId --- I am an existing note --- $randomId")
  )

  def testDelete(
    existingNote: Option[String] = None,
    testNonExistingFile: Boolean = false,
    force: Boolean = false,
    workspaceName: Option[String] = None
  ): Future[Assertion] = {
    val deleteNote = s"$randomId --- Deleted by the integration tests --- $randomId"

    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfContents = s"$id --- I am a vcf fated to die --- $id"
    val vcfIndexContents = s"$id --- I am an index fated to die --- $id"
    val gtcContents = s"$id --- I am an gtc fated to die --- $id"
    val paramsContents = s"$id --- I am an params fated to die --- $id"
    val metrics1Contents = s"$id --- I am an immortal metrics file --- $id"
    val metrics2Contents =
      s"$id --- I am a second immortal metrics file --- $id"

    val storageDir = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfPath = storageDir / s"$id${ArraysExtensions.VcfGzExtension}"
    val vcfIndexPath = storageDir / s"$id${ArraysExtensions.VcfGzTbiExtension}"
    val gtcPath = storageDir / s"gtc-$id"
    val paramsPath = storageDir / s"params-$id${ArraysExtensions.TxtExtension}"
    val metrics1Path = storageDir / s"detail-$id${ArraysExtensions.FingerprintingDetailMetricsExtension}"
    val metrics2Path = storageDir / s"summary-$id${ArraysExtensions.FingerprintingSummaryMetricsExtension}"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfPath.uri),
      vcfIndexPath = Some(vcfIndexPath.uri),
      gtcPath = Some(gtcPath.uri),
      paramsPath = Some(paramsPath.uri),
      fingerprintingDetailMetricsPath = Some(metrics1Path.uri),
      fingerprintingSummaryMetricsPath = Some(metrics2Path.uri),
      notes = existingNote,
      workspaceName = workspaceName
    )

    val _ = if (!testNonExistingFile) {
      Seq(
        (vcfPath, vcfContents),
        (vcfIndexPath, vcfIndexContents),
        (gtcPath, gtcContents),
        (paramsPath, paramsContents),
        (metrics1Path, metrics1Contents),
        (metrics2Path, metrics2Contents)
      ).map {
        case (path, contents) => path.write(contents)
      }
    }

    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runIgnore(
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
      outputs <- runCollectJson(
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
      Seq(vcfPath, vcfIndexPath, gtcPath, paramsPath).foreach(_ shouldNot exist)
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
      runIgnore(
        ClioCommand.deleteArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        randomId,
        "--version",
        "123"
      )
    }.map {
      _.getMessage should include("--note")
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

  it should "move files and record the workspace name when delivering" in testDeliver()

  it should "deliver using a custom billing project when given one" in testDeliver(
    customBillingProject = true
  )

  it should "support changing the vcf, vcfIndex, and gtc basename on deliver" in testDeliver(
    changeBasename = true
  )

  def testDeliver(
    changeBasename: Boolean = false,
    customBillingProject: Boolean = false
  ): Future[Assertion] = {
    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfContents = s"$id --- I am a dummy vcf --- $id"
    val vcfIndexContents = s"$id --- I am a dummy vcfIndex --- $id"
    val gtcContents = s"$id --- I am a dummy gtc --- $id"
    val grnIdatContents = s"$id --- I am a dummy grn idat --- $id"
    val redIdatContents = s"$id --- I am a dummy red idat --- $id"
    val variantCallingSummaryMetricsContents =
      s"$id --- I am dummy variant_calling_summary_metrics --- $id"

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$barcode${ArraysExtensions.VcfGzTbiExtension}"
    val gtcName = s"$barcode${ArraysExtensions.GtcExtension}"
    val grnIdatName = s"grn-$barcode${ArraysExtensions.GrnIdatExtension}"
    val redIdatName = s"red-$barcode${ArraysExtensions.RedIdatExtension}"

    val rootSource = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName
    val gtcSource = rootSource / gtcName
    val grnIdatSource = rootSource / grnIdatName
    val redIdatSource = rootSource / redIdatName
    val variantCallingSummaryMetricsSource = rootSource / s"$barcode${ArraysExtensions.VariantCallingSummaryMetricsExtension}"

    val endBasename = if (changeBasename) randomId else barcode

    val rootDestination = rootSource.parent / s"moved/$id/"
    val vcfDestination = rootDestination / s"$endBasename${ArraysExtensions.VcfGzExtension}"
    val vcfIndexDestination = rootDestination / s"$endBasename${ArraysExtensions.VcfGzTbiExtension}"
    val gtcDestination = rootDestination / s"$endBasename${ArraysExtensions.GtcExtension}"
    val grnIdatDestination = rootDestination / "idats" / grnIdatName
    val redIdatDestination = rootDestination / "idats" / redIdatName
    val variantCallingSummaryMetricsDestination = rootDestination / s"$endBasename${ArraysExtensions.VariantCallingSummaryMetricsExtension}"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      gtcPath = Some(gtcSource.uri),
      grnIdatPath = Some(grnIdatSource.uri),
      redIdatPath = Some(redIdatSource.uri),
      variantCallingSummaryMetricsPath = Some(variantCallingSummaryMetricsSource.uri),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$id-TestWorkspace-$id"
    val billingProject =
      if (customBillingProject) s"$id-TestBillingProject-$id"
      else ClioCommand.defaultBillingProject

    val _ = Seq(
      (vcfSource, vcfContents),
      (vcfIndexSource, vcfIndexContents),
      (gtcSource, gtcContents),
      (grnIdatSource, grnIdatContents),
      (redIdatSource, redIdatContents),
      (variantCallingSummaryMetricsSource, variantCallingSummaryMetricsContents)
    ).map {
      case (source, contents) => source.write(contents)
    }

    val customBillingProjectArgs =
      if (customBillingProject) {
        Seq("--billing-project", billingProject)
      } else {
        Seq.empty
      }

    val args = Seq.concat(
      Seq(
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
      ) ++ customBillingProjectArgs,
      if (changeBasename) {
        Seq("--new-basename", endBasename)
      } else {
        Seq.empty
      }
    )

    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runIgnore(ClioCommand.deliverArraysName, args: _*)
      outputs <- runCollectJson(
        ClioCommand.queryArraysName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(vcfSource, vcfIndexSource, gtcSource).foreach(_ shouldNot exist)

      Seq(
        vcfDestination,
        vcfIndexDestination,
        gtcDestination,
        grnIdatSource,
        redIdatSource,
        grnIdatDestination,
        redIdatDestination,
        variantCallingSummaryMetricsSource // Note - we do not move metrics during delivery.  Source should exist
      ).foreach(_ should exist)

      Seq(
        variantCallingSummaryMetricsDestination
      ).foreach(_ shouldNot exist) // Note - we do not move metrics during delivery.  Destination should NOT exist

      Seq(
        (vcfDestination, vcfContents),
        (vcfIndexDestination, vcfIndexContents),
        (gtcDestination, gtcContents),
        (grnIdatDestination, grnIdatContents),
        (redIdatDestination, redIdatContents)
      ).foreach {
        case (destination, contents) =>
          destination.contentAsString should be(contents)
      }

      outputs should contain only expectedMerge(
        key,
        metadata.copy(
          workspaceName = Some(workspaceName),
          billingProject = Some(billingProject),
          vcfPath = Some(vcfDestination.uri),
          vcfIndexPath = Some(vcfIndexDestination.uri),
          gtcPath = Some(gtcDestination.uri),
          grnIdatPath = Some(grnIdatDestination.uri),
          redIdatPath = Some(redIdatDestination.uri)
        )
      )
    }

    result.andThen {
      case _ =>
        val _ = Seq(
          vcfSource,
          vcfDestination,
          vcfIndexSource,
          vcfIndexDestination,
          gtcSource,
          gtcDestination,
          grnIdatSource,
          grnIdatDestination,
          redIdatSource,
          redIdatDestination
        ).map(_.delete(swallowIOExceptions = true))
    }
  }

  it should "not fail delivery if the vcf is already in its target location" in {
    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfContents = s"$id --- I am a dummy vcf --- $id"
    val vcfIndexContents = s"$id --- I am a dummy vcfIndex --- $id"
    val gtcContents = s"$id --- I am a dummy gtc --- $id"
    val grnIdatContents = s"$id --- I am a dummy grn idat --- $id"
    val redIdatContents = s"$id --- I am a dummy red idat --- $id"

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$barcode${ArraysExtensions.VcfGzTbiExtension}"
    val gtcName = s"$barcode${ArraysExtensions.GtcExtension}"
    val grnIdatName = s"grn-$id${ArraysExtensions.GrnIdatExtension}"
    val redIdatName = s"red-$id${ArraysExtensions.RedIdatExtension}"

    val rootSource = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName
    val gtcSource = rootSource / gtcName
    val grnIdatSource = rootSource / "idats" / grnIdatName
    val redIdatSource = rootSource / "idats" / redIdatName

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      gtcPath = Some(gtcSource.uri),
      grnIdatPath = Some(grnIdatSource.uri),
      redIdatPath = Some(redIdatSource.uri),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$id-TestWorkspace-$id"

    val _ = Seq(
      (vcfSource, vcfContents),
      (vcfIndexSource, vcfIndexContents),
      (gtcSource, gtcContents),
      (grnIdatSource, grnIdatContents),
      (redIdatSource, redIdatContents)
    ).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runIgnore(
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
      outputs <- runCollectJson(
        ClioCommand.queryArraysName,
        "--workspace-name",
        workspaceName
      )
    } yield {
      Seq(vcfSource, vcfIndexSource, gtcSource).foreach(_ should exist)

      Seq(
        (vcfSource, vcfContents),
        (vcfIndexSource, vcfIndexContents),
        (gtcSource, gtcContents),
        (grnIdatSource, grnIdatContents),
        (redIdatSource, redIdatContents)
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
      case _ =>
        val _ = Seq(vcfSource, vcfIndexSource)
          .map(_.delete(swallowIOExceptions = true))
    }
  }

  it should "fail delivery if the underlying move fails" in {
    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$barcode${ArraysExtensions.VcfGzTbiExtension}"
    val gtcName = s"$barcode${ArraysExtensions.GtcExtension}"
    val grnIdatName = s"grn-$id${ArraysExtensions.IdatExtension}"
    val redIdatName = s"red-$id${ArraysExtensions.IdatExtension}"

    val rootSource = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName
    val gtcSource = rootSource / gtcName
    val grnIdatSource = rootSource / grnIdatName
    val redIdatSource = rootSource / redIdatName

    val rootDestination = rootSource.parent / s"moved/$id/"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      gtcPath = Some(gtcSource.uri),
      grnIdatPath = Some(grnIdatSource.uri),
      redIdatPath = Some(redIdatSource.uri)
    )

    val workspaceName = s"$id-TestWorkspace-$id"

    recoverToExceptionIf[Exception] {
      for {
        _ <- runUpsertArrays(key, metadata)
        // Should fail because the source files don't exist.
        deliverResponse <- runDecode[UpsertId](
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
        outputs <- runCollectJson(
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

  it should "fail delivery if gtc file is missing" in {
    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$barcode${ArraysExtensions.VcfGzTbiExtension}"
    val grnIdatName = s"grn-$id${ArraysExtensions.IdatExtension}"
    val redIdatName = s"red-$id${ArraysExtensions.IdatExtension}"

    val rootSource = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName
    val grnIdatSource = rootSource / grnIdatName
    val redIdatSource = rootSource / redIdatName

    val rootDestination = rootSource.parent / s"moved/$randomId/"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      grnIdatPath = Some(grnIdatSource.uri),
      redIdatPath = Some(redIdatSource.uri)
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    recoverToExceptionIf[Exception] {
      for {
        _ <- runUpsertArrays(key, metadata)
        // Should fail because the idat files aren't set.
        deliverResponse <- runDecode[UpsertId](
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
        outputs <- runCollectJson(
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

  it should "fail delivery if idat files are missing" in {
    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$barcode${ArraysExtensions.VcfGzTbiExtension}"
    val gtcName = s"$barcode${ArraysExtensions.GtcExtension}"

    val rootSource = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName
    val gtcSource = rootSource / gtcName

    val rootDestination = rootSource.parent / s"moved/$randomId/"

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      gtcPath = Some(gtcSource.uri)
    )

    val workspaceName = s"$randomId-TestWorkspace-$randomId"

    recoverToExceptionIf[Exception] {
      for {
        _ <- runUpsertArrays(key, metadata)
        // Should fail because the idat files aren't set.
        deliverResponse <- runDecode[UpsertId](
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
        outputs <- runCollectJson(
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

  it should "move instead of copying idat files when re-delivering" in {
    val id = randomId
    val barcode = s"barcode$id"
    val version = 3

    val vcfContents = s"$id --- I am a dummy vcf --- $id"
    val vcfIndexContents = s"$id --- I am a dummy vcfIndex --- $id"
    val gtcContents = s"$id --- I am a dummy gtc --- $id"
    val grnIdatContents = s"$id --- I am a dummy grn idat --- $id"
    val redIdatContents = s"$id --- I am a dummy red idat --- $id"

    val vcfName = s"$barcode${ArraysExtensions.VcfGzExtension}"
    val vcfIndexName = s"$barcode${ArraysExtensions.VcfGzTbiExtension}"
    val gtcName = s"$barcode${ArraysExtensions.GtcExtension}"
    val grnIdatName = s"grn-$id${ArraysExtensions.GrnIdatExtension}"
    val redIdatName = s"red-$id${ArraysExtensions.RedIdatExtension}"

    val rootSource = rootTestStorageDir / s"arrays/$barcode/v$version/"
    val vcfSource = rootSource / vcfName
    val vcfIndexSource = rootSource / vcfIndexName
    val gtcSource = rootSource / gtcName
    val grnIdatSource = rootSource / "idats" / grnIdatName
    val redIdatSource = rootSource / "idats" / redIdatName

    val rootDestination = rootSource.parent / s"moved/$randomId/"
    val rootDestination2 = rootSource.parent / s"moved2/$randomId/"
    val grnIdatDestination = rootDestination / "idats" / grnIdatName
    val redIdatDestination = rootDestination / "idats" / redIdatName
    val grnIdatDestination2 = rootDestination2 / "idats" / grnIdatName
    val redIdatDestination2 = rootDestination2 / "idats" / redIdatName

    val key = ArraysKey(Location.GCP, Symbol(barcode), version)
    val metadata = ArraysMetadata(
      vcfPath = Some(vcfSource.uri),
      vcfIndexPath = Some(vcfIndexSource.uri),
      gtcPath = Some(gtcSource.uri),
      grnIdatPath = Some(grnIdatSource.uri),
      redIdatPath = Some(redIdatSource.uri),
      documentStatus = Some(DocumentStatus.Normal)
    )

    val workspaceName = s"$id-TestWorkspace-$id"
    val workspaceName2 = s"$id-TestWorkspace2-$id"

    val _ = Seq(
      (vcfSource, vcfContents),
      (vcfIndexSource, vcfIndexContents),
      (gtcSource, gtcContents),
      (grnIdatSource, grnIdatContents),
      (redIdatSource, redIdatContents)
    ).map {
      case (source, contents) => source.write(contents)
    }
    val result = for {
      _ <- runUpsertArrays(key, metadata)
      _ <- runIgnore(
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
      outputs <- runCollectJson(
        ClioCommand.queryArraysName,
        "--workspace-name",
        workspaceName
      )
      _ <- runIgnore(
        ClioCommand.deliverArraysName,
        "--location",
        Location.GCP.entryName,
        "--chipwell-barcode",
        barcode,
        "--version",
        version.toString,
        "--workspace-name",
        workspaceName2,
        "--workspace-path",
        rootDestination2.uri.toString,
        "--force"
      )
      outputs2 <- runCollectJson(
        ClioCommand.queryArraysName,
        "--workspace-name",
        workspaceName2
      )
    } yield {
      // The source files should exist because they were copied initially
      grnIdatSource should exist
      redIdatSource should exist

      // The idats in the first workspace should not exist because they were moved instead of copied
      grnIdatDestination should not(exist)
      redIdatDestination should not(exist)

      // The idats should exist in the second workspace
      grnIdatDestination2 should exist
      redIdatDestination2 should exist
    }

    result.andThen {
      case _ =>
        val _ = Seq(vcfSource, vcfIndexSource)
          .map(_.delete(swallowIOExceptions = true))
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

  it should "patch documents with new metadata" in {

    val metadataFile = File
      .newTemporaryFile()
      .write(
        ArraysMetadata(
          sampleAlias = Some("patched_sample_alias"),
          chipType = Some("patched_chip_type")
        ).asJson.printWith(implicitly)
      )

    val upsertKey1 = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"barcode$randomId"),
      version = 1
    )
    val upsertKey2 = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"barcode$randomId"),
      version = 2
    )

    for {
      _ <- runUpsertArrays(
        upsertKey1,
        ArraysMetadata()
      )
      _ <- runUpsertArrays(
        upsertKey2,
        ArraysMetadata(
          chipType = Some("existing_chip_type")
        )
      )
      _ <- runIgnore(
        ClioCommand.patchArraysName,
        "--metadata-location",
        metadataFile.toString()
      )
      patched1 <- runCollectJson(
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        upsertKey1.chipwellBarcode.name,
        "--version",
        upsertKey1.version.toString
      )
      patched2 <- runCollectJson(
        ClioCommand.queryArraysName,
        "--chipwell-barcode",
        upsertKey2.chipwellBarcode.name,
        "--version",
        upsertKey2.version.toString
      )
    } yield {

      val storedDocument1 = patched1.headOption.getOrElse(fail)
      storedDocument1.unsafeGet[String]("chip_type") should be("patched_chip_type")
      storedDocument1.unsafeGet[String]("sample_alias") should be(
        "patched_sample_alias"
      )

      val storedDocument2 = patched2.headOption.getOrElse(fail)
      storedDocument2.unsafeGet[String]("chip_type") should be("existing_chip_type")
      storedDocument2.unsafeGet[String]("sample_alias") should be(
        "patched_sample_alias"
      )
    }
  }

  it should "write dict, fasta, fasta.fai paths" in {
    val upsertKey = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"barcode$randomId"),
      version = 1
    )
    val referencesDirs = "hg19/v0/Homo_sapiens_assembly19"
    val refDictPath = rootPathForReferencesBucket / s"$referencesDirs${ArraysExtensions.DictExtension}"
    val refFastaPath = rootPathForReferencesBucket / s"$referencesDirs${ArraysExtensions.FastaExtension}"
    val refFastaIndexPath = rootPathForReferencesBucket / s"$referencesDirs${ArraysExtensions.FastaFaiExtension}"
    for {
      upsertId <- runUpsertArrays(
        upsertKey,
        ArraysMetadata(
          refDictPath = Some(refDictPath.uri),
          refFastaPath = Some(refFastaPath.uri),
          refFastaIndexPath = Some(refFastaIndexPath.uri)
        ),
        force = false
      )
    } yield {
      val storedDocument = getJsonFrom(upsertId)(ElasticsearchIndex.Arrays)
      storedDocument.unsafeGet[URI]("ref_dict_path") should be(refDictPath.uri)
      storedDocument.unsafeGet[URI]("ref_fasta_path") should be(refFastaPath.uri)
      storedDocument.unsafeGet[URI]("ref_fasta_index_path") should be(
        refFastaIndexPath.uri
      )
    }
  }
}
