package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation,
  UpsertId
}

case class DocumentGvcf(upsertId: UpsertId,
                        entityId: Symbol,
                        location: Location,
                        project: String,
                        sampleAlias: String,
                        version: Int,
                        analysisDate: Option[OffsetDateTime] = None,
                        contamination: Option[Float] = None,
                        documentStatus: Option[DocumentStatus] = None,
                        gvcfMd5: Option[Symbol] = None,
                        gvcfPath: Option[URI] = None,
                        gvcfSize: Option[Long] = None,
                        gvcfIndexPath: Option[URI] = None,
                        gvcfSummaryMetricsPath: Option[URI] = None,
                        gvcfDetailMetricsPath: Option[URI] = None,
                        pipelineVersion: Option[Symbol] = None,
                        regulatoryDesignation: Option[RegulatoryDesignation] =
                          None,
                        notes: Option[String] = None)
    extends ClioDocument
