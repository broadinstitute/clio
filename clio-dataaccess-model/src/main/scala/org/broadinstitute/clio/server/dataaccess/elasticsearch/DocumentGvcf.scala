package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

case class DocumentGvcf(clioId: UUID,
                        entityId: String,
                        location: Location,
                        project: String,
                        sampleAlias: String,
                        version: Int,
                        analysisDate: Option[OffsetDateTime],
                        contamination: Option[Float],
                        documentStatus: Option[DocumentStatus],
                        gvcfMd5: Option[String],
                        gvcfPath: Option[String],
                        gvcfSize: Option[Long],
                        gvcfMetricsPath: Option[String],
                        pipelineVersion: Option[Long],
                        notes: Option[String])
    extends ClioDocument
