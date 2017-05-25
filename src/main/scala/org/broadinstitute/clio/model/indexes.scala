package org.broadinstitute.clio.model

case class ElasticsearchIndex(indexName: String, indexType: String, fields: Seq[ElasticsearchField])

case class ElasticsearchField(fieldName: String, fieldType: Class[_])

case class ReadGroup
(
  project: String,
  sampleAlias: String,
  flowcellBarcode: String,
  lane: Int,
  libraryName: String,
  molecularBarcodeName: String,
  pairedRun: Boolean,
  ubamPath: String,
  ubamSize: Long,
  ubamMd5: String
)
