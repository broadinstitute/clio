package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import java.time.OffsetDateTime
import java.util.UUID

case class DocumentWgsUbam(clioId: UUID,
                           flowcellBarcode: String,
                           lane: Int,
                           libraryName: String,
                           location: Location,
                           analysisType: Option[String],
                           baitIntervals: Option[String],
                           dataType: Option[String],
                           individualAlias: Option[String],
                           initiative: Option[String],
                           lcSet: Option[String],
                           libraryType: Option[String],
                           machineName: Option[String],
                           molecularBarcodeName: Option[String],
                           molecularBarcodeSequence: Option[String],
                           pairedRun: Option[Boolean],
                           productFamily: Option[String],
                           productName: Option[String],
                           productOrderId: Option[String],
                           productPartNumber: Option[String],
                           project: Option[String],
                           readStructure: Option[String],
                           researchProjectId: Option[String],
                           researchProjectName: Option[String],
                           rootSampleId: Option[String],
                           runDate: Option[OffsetDateTime],
                           runName: Option[String],
                           sampleAlias: Option[String],
                           sampleGender: Option[String],
                           sampleId: Option[String],
                           sampleLsid: Option[String],
                           sampleType: Option[String],
                           targetIntervals: Option[String],
                           notes: Option[String],
                           ubamMd5: Option[String],
                           ubamPath: Option[String],
                           ubamSize: Option[Long],
                           documentStatus: Option[DocumentStatus])
    extends ClioDocument
