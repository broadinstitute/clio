package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.MemoryReadGroupSearchDAO
import org.broadinstitute.clio.server.model.{
  ModelReadGroupKey,
  ModelReadGroupLocation,
  ModelReadGroupMetadata,
  ModelReadGroupQueryInput
}
import org.broadinstitute.clio.transfer.model.{
  TransferReadGroupV1Key,
  TransferReadGroupV1Metadata,
  TransferReadGroupV1QueryInput
}
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.scalatest.{AsyncFlatSpec, Matchers}

class ReadGroupServiceSpec extends AsyncFlatSpec with Matchers {
  behavior of "ReadGroupService"

  it should "upsertMetadata" in {
    val memorySearchDAO = new MemoryReadGroupSearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val readGroupService = ReadGroupService(app)
    val transferKey = TransferReadGroupV1Key("barcode1", 2, "library3")
    val transferMetadataMapper =
      new CaseClassMapper[TransferReadGroupV1Metadata]
    val transferMetadata =
      transferMetadataMapper.newInstance(
        Map("project" -> Option("testProject"))
      )
    for {
      _ <- readGroupService.upsertMetadata(transferKey, transferMetadata)
    } yield {
      memorySearchDAO.updateReadGroupMetadataCalls should be(
        Seq(
          (
            ModelReadGroupKey(
              "barcode1",
              2,
              "library3",
              ModelReadGroupLocation.unknown._2
            ),
            ModelReadGroupMetadata(
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
              ubamMd5 = None,
              ubamPath = None,
              ubamSize = None
            )
          )
        )
      )
      memorySearchDAO.queryReadGroupCalls should be(empty)
    }
  }

  it should "queryData" in {
    val memorySearchDAO = new MemoryReadGroupSearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val readGroupService = ReadGroupService(app)
    val transferInputMapper =
      new CaseClassMapper[TransferReadGroupV1QueryInput]
    val transferInput =
      transferInputMapper.newInstance(Map("project" -> Option("testProject")))
    for {
      _ <- readGroupService.queryMetadata(transferInput)
    } yield {
      memorySearchDAO.updateReadGroupMetadataCalls should be(empty)
      memorySearchDAO.queryReadGroupCalls should be(
        Seq(
          ModelReadGroupQueryInput(
            flowcellBarcode = None,
            lane = None,
            libraryName = None,
            location = None,
            lcSet = None,
            project = Option("testProject"),
            runDateEnd = None,
            runDateStart = None,
            sampleAlias = None
          )
        )
      )
    }
  }
}
