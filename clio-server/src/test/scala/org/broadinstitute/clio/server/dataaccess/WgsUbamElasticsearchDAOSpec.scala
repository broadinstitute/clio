package org.broadinstitute.clio.server.dataaccess

import java.util.UUID
import org.broadinstitute.clio.server.model._
import org.broadinstitute.clio.util.model.Location

import org.scalatest.{AsyncFlatSpecLike, Matchers}

class WgsUbamElasticsearchDAOSpec
    extends AbstractElasticsearchDAOSpec("WgsUbamElasticsearchDAOSpec")
    with AsyncFlatSpecLike
    with Matchers {

  behavior of "WgsUbamElasticsearchDAO"

  lazy val wgsUbamElasticsearchDAO: WgsUbamElasticsearchDAO =
    httpElasticsearchDAO

  it should "initialize" in {
    initialize()
  }

  it should "updateWgsUbamMetadata" in {
    val key = ModelWgsUbamKey("barcodeURGM1", 2, "library3", Location.GCP)
    val metadata = ModelWgsUbamMetadata(
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
      notes = None,
      ubamMd5 = None,
      ubamPath = None,
      ubamSize = None,
      documentStatus = None
    )
    for {
      _ <- wgsUbamElasticsearchDAO.updateWgsUbamMetadata(key, metadata)
    } yield {
      succeed
    }
  }

  it should "queryWgsUbam" in {
    val id = UUID.randomUUID.toString.replaceAll("-", "")
    val library = "library" + id
    val key = ModelWgsUbamKey("barcodeQRG1", 2, library, Location.OnPrem)
    val metadata = ModelWgsUbamMetadata(
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
      notes = None,
      ubamMd5 = Option("md5"),
      ubamPath = Option(s"expected_path_$id"),
      ubamSize = Option(12345L),
      documentStatus = None
    )
    val queryInput = ModelWgsUbamQueryInput(
      flowcellBarcode = None,
      lane = None,
      libraryName = Option(library),
      location = None,
      lcSet = None,
      project = None,
      runDateEnd = None,
      runDateStart = None,
      sampleAlias = None,
      documentStatus = None
    )
    for {
      _ <- wgsUbamElasticsearchDAO.updateWgsUbamMetadata(key, metadata)
      queryOutputs <- wgsUbamElasticsearchDAO.queryWgsUbam(queryInput)
    } yield {
      queryOutputs should be(
        Seq(
          ModelWgsUbamQueryOutput(
            flowcellBarcode = "barcodeQRG1",
            lane = 2,
            libraryName = library,
            location = Location.OnPrem,
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
            notes = None,
            ubamMd5 = Option("md5"),
            ubamPath = Option(s"expected_path_$id"),
            ubamSize = Option(12345L),
            documentStatus = None
          )
        )
      )
    }
  }
}
