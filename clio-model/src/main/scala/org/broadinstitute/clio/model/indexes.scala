package org.broadinstitute.clio.model

import java.time.OffsetDateTime

case class ElasticsearchIndex(indexName: String,
                              indexType: String,
                              fields: Seq[ElasticsearchField])

case class ElasticsearchField(fieldName: String, fieldType: Class[_])

case class ReadGroup(analysisType: String,
                     baitIntervals: String,
                     dataType: String,
                     flowcellBarcode: String,
                     individualAlias: String,
                     initiative: String,
                     lane: Int,
                     lcSet: String,
                     libraryName: String,
                     libraryType: String,
                     machineName: String,
                     molecularBarcodeName: String,
                     molecularBarcodeSequence: String,
                     pairedRun: Boolean,
                     productFamily: String,
                     productName: String,
                     productOrderId: String,
                     productPartNumber: String,
                     project: String,
                     readStructure: String,
                     researchProjectId: String,
                     researchProjectName: String,
                     rootSampleId: String,
                     runDate: OffsetDateTime,
                     runName: String,
                     sampleAlias: String,
                     sampleGender: String,
                     sampleId: String,
                     sampleLsid: String,
                     sampleType: String,
                     targetIntervals: String,
                     ubamMd5: String,
                     ubamPath: String,
                     ubamSize: Long)
