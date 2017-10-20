package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}

case class DocumentGvcf(upsertId: UpsertId,
                        entityId: String,
                        location: Location,
                        project: String,
                        sampleAlias: String,
                        version: Int,
                        analysisDate: Option[OffsetDateTime] = None,
                        contamination: Option[Float] = None,
                        documentStatus: Option[DocumentStatus] = None,
                        gvcfMd5: Option[String] = None,
                        gvcfPath: Option[String] = None,
                        gvcfSize: Option[Long] = None,
                        gvcfIndexPath: Option[String] = None,
                        gvcfSummaryMetricsPath: Option[String] = None,
                        gvcfDetailMetricsPath: Option[String] = None,
                        pipelineVersion: Option[String] = None,
                        notes: Option[String] = None)
    extends ClioDocument
