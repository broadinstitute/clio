package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.MemoryWgsUbamSearchDAO
import org.broadinstitute.clio.server.model._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import org.scalatest.{AsyncFlatSpec, Matchers}

class WgsUbamServiceSpec extends AsyncFlatSpec with Matchers {
  behavior of "WgsUbamService"

  it should "upsertMetadata" in {
    upsertMetadataTest(None, Option(DocumentStatus.Normal))
  }

  it should "upsertMetadata with document_status explicitly set to Normal" in {
    upsertMetadataTest(
      Option(DocumentStatus.Normal),
      Option(DocumentStatus.Normal)
    )
  }

  it should "upsertMetadata with document_status explicitly set to Deleted" in {
    upsertMetadataTest(
      Option(DocumentStatus.Deleted),
      Option(DocumentStatus.Deleted)
    )
  }

  it should "queryData" in {
    val memorySearchDAO = new MemoryWgsUbamSearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val wgsUbamService = WgsUbamService(app)
    val transferInputMapper =
      new CaseClassMapper[TransferWgsUbamV1QueryInput]
    val transferInput =
      transferInputMapper.newInstance(Map("project" -> Option("testProject")))
    for {
      _ <- wgsUbamService.queryMetadata(transferInput)
    } yield {
      memorySearchDAO.updateWgsUbamMetadataCalls should be(empty)
      memorySearchDAO.queryWgsUbamCalls should be(
        Seq(
          ModelWgsUbamQueryInput(
            flowcellBarcode = None,
            lane = None,
            libraryName = None,
            location = None,
            lcSet = None,
            project = Option("testProject"),
            runDateEnd = None,
            runDateStart = None,
            sampleAlias = None,
            documentStatus = Option(DocumentStatus.Normal)
          )
        )
      )
    }
  }

  private def upsertMetadataTest(
    documentStatus: Option[DocumentStatus],
    expectedDocumentStatus: Option[DocumentStatus]
  ) = {
    val memorySearchDAO = new MemoryWgsUbamSearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val wgsUbamService = WgsUbamService(app)
    val transferKey =
      TransferWgsUbamV1Key("barcode1", 2, "library3", Location.GCP)
    val transferMetadataMapper =
      new CaseClassMapper[TransferWgsUbamV1Metadata]
    val transferMetadata =
      transferMetadataMapper.newInstance(
        Map(
          "project" -> Option("testProject"),
          "notes" -> Option("notable update"),
          "documentStatus" -> documentStatus
        )
      )
    for {
      returnedClioId <- wgsUbamService.upsertMetadata(
        transferKey,
        transferMetadata
      )
    } yield {
      memorySearchDAO.updateWgsUbamMetadataCalls should be(
        Seq(
          (
            ModelWgsUbamKey("barcode1", 2, "library3", Location.GCP),
            ModelWgsUbamMetadata(
              clioId = Some(returnedClioId),
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
              project = Option("testProject"),
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
              notes = Option("notable update"),
              ubamMd5 = None,
              ubamPath = None,
              ubamSize = None,
              documentStatus = expectedDocumentStatus
            )
          )
        )
      )
      memorySearchDAO.queryWgsUbamCalls should be(empty)
    }

  }
}
