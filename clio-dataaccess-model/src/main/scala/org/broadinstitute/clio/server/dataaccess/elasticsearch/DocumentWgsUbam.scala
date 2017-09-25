package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import java.time.OffsetDateTime
import java.util.UUID

case class DocumentWgsUbam(upsertId: UUID,
                           entityId: String,
                           flowcellBarcode: String,
                           lane: Int,
                           libraryName: String,
                           location: Location,
                           analysisType: Option[String] = None,
                           baitIntervals: Option[String] = None,
                           dataType: Option[String] = None,
                           individualAlias: Option[String] = None,
                           initiative: Option[String] = None,
                           lcSet: Option[String] = None,
                           libraryType: Option[String] = None,
                           machineName: Option[String] = None,
                           molecularBarcodeName: Option[String] = None,
                           molecularBarcodeSequence: Option[String] = None,
                           pairedRun: Option[Boolean] = None,
                           productFamily: Option[String] = None,
                           productName: Option[String] = None,
                           productOrderId: Option[String] = None,
                           productPartNumber: Option[String] = None,
                           project: Option[String] = None,
                           readStructure: Option[String] = None,
                           researchProjectId: Option[String] = None,
                           researchProjectName: Option[String] = None,
                           rootSampleId: Option[String] = None,
                           runDate: Option[OffsetDateTime] = None,
                           runName: Option[String] = None,
                           sampleAlias: Option[String] = None,
                           sampleGender: Option[String] = None,
                           sampleId: Option[String] = None,
                           sampleLsid: Option[String] = None,
                           sampleType: Option[String] = None,
                           targetIntervals: Option[String] = None,
                           notes: Option[String] = None,
                           ubamMd5: Option[String] = None,
                           ubamPath: Option[String] = None,
                           ubamSize: Option[Long] = None,
                           documentStatus: Option[DocumentStatus] = None)
    extends ClioDocument
