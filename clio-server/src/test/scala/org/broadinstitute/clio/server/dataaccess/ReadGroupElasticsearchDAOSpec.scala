package org.broadinstitute.clio.server.dataaccess

import java.util.UUID

import org.broadinstitute.clio.server.model._
import org.scalatest.{AsyncFlatSpecLike, Matchers}

class ReadGroupElasticsearchDAOSpec
    extends AbstractElasticsearchDAOSpec("ReadGroupElasticsearchDAOSpec")
    with AsyncFlatSpecLike
    with Matchers {

  behavior of "ReadGroupElasticsearchDAO"

  lazy val readGroupElasticsearchDAO: ReadGroupElasticsearchDAO =
    httpElasticsearchDAO

  it should "initialize" in {
    initialize()
  }

  it should "updateReadGroupMetadata" in {
    val key = ModelReadGroupKey("barcodeURGM1", 2, "library3")
    val metadata = ModelReadGroupMetadata(
      analysisType = None,
      baitIntervals = None,
      dataType = None,
      individualAlias = None,
      initiative = None,
      lcSet = None,
      libraryType = None,
      machineName = None,
      molecularBarcodeName = None,
      molecularBarcodeSequence = None,
      pairedRun = None,
      productFamily = None,
      productName = None,
      productOrderId = None,
      productPartNumber = None,
      project = Option("updatedProject"),
      readStructure = None,
      researchProjectId = None,
      researchProjectName = None,
      rootSampleId = None,
      runDate = None,
      runName = None,
      sampleAlias = None,
      sampleGender = None,
      sampleId = None,
      sampleLsid = None,
      sampleType = None,
      targetIntervals = None,
      ubamMd5 = None,
      ubamPath = None,
      ubamSize = None
    )
    for {
      _ <- readGroupElasticsearchDAO.updateReadGroupMetadata(key, metadata)
    } yield {
      succeed
    }
  }

  it should "queryReadGroup" in {
    val id = UUID.randomUUID.toString.replaceAll("-", "")
    val library = "library" + id
    val key = ModelReadGroupKey("barcodeQRG1", 2, library)
    val metadata = ModelReadGroupMetadata(
      analysisType = None,
      baitIntervals = None,
      dataType = None,
      individualAlias = None,
      initiative = None,
      lcSet = None,
      libraryType = None,
      machineName = None,
      molecularBarcodeName = None,
      molecularBarcodeSequence = None,
      pairedRun = None,
      productFamily = None,
      productName = None,
      productOrderId = None,
      productPartNumber = None,
      project = Option("updatedProject"),
      readStructure = None,
      researchProjectId = None,
      researchProjectName = None,
      rootSampleId = None,
      runDate = None,
      runName = None,
      sampleAlias = None,
      sampleGender = None,
      sampleId = None,
      sampleLsid = None,
      sampleType = None,
      targetIntervals = None,
      ubamMd5 = Option("md5"),
      ubamPath = Option(s"expected_path_$id"),
      ubamSize = Option(12345L)
    )
    val queryInput = ModelReadGroupQueryInput(
      flowcellBarcode = None,
      lane = None,
      lcSet = None,
      libraryName = Option(library),
      project = None,
      runDateEnd = None,
      runDateStart = None,
      sampleAlias = None
    )
    for {
      _ <- readGroupElasticsearchDAO.updateReadGroupMetadata(key, metadata)
      queryOutputs <- readGroupElasticsearchDAO.queryReadGroup(queryInput)
    } yield {
      queryOutputs should be(
        Seq(
          ModelReadGroupQueryOutput(
            analysisType = None,
            baitIntervals = None,
            dataType = None,
            flowcellBarcode = "barcodeQRG1",
            individualAlias = None,
            initiative = None,
            lane = 2,
            lcSet = None,
            libraryName = library,
            libraryType = None,
            machineName = None,
            molecularBarcodeName = None,
            molecularBarcodeSequence = None,
            pairedRun = None,
            productFamily = None,
            productName = None,
            productOrderId = None,
            productPartNumber = None,
            project = Option("updatedProject"),
            readStructure = None,
            researchProjectId = None,
            researchProjectName = None,
            rootSampleId = None,
            runDate = None,
            runName = None,
            sampleAlias = None,
            sampleGender = None,
            sampleId = None,
            sampleLsid = None,
            sampleType = None,
            targetIntervals = None,
            ubamMd5 = Option("md5"),
            ubamPath = Option(s"expected_path_$id"),
            ubamSize = Option(12345L)
          )
        )
      )
    }
  }

}