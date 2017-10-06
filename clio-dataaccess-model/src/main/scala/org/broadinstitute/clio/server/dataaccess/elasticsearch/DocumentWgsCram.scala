package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}

case class DocumentWgsCram(upsertId: UpsertId,
                           entityId: String,
                           documentStatus: DocumentStatus,
                           location: Location,
                           project: String,
                           sampleAlias: String,
                           version: Int,
                           pipelineVersion: Option[Long] = None,
                           workflowStartDate: Option[OffsetDateTime] = None,
                           workflowEndDate: Option[OffsetDateTime] = None,
                           cramMd5: Option[String] = None,
                           cramSize: Option[Long] = None,
                           cramPath: Option[String] = None,
                           craiPath: Option[String] = None,
                           logPath: Option[String] = None,
                           fingerprintPath: Option[String] = None,
                           cromwellId: Option[String] = None,
                           workflowJsonPath: Option[String] = None,
                           optionsJsonPath: Option[String] = None,
                           wdlPath: Option[String] = None,
                           readGroupMd5: Option[String] = None,
                           analysisFiles: List[String] = List(),
                           notes: Option[String] = None)
  extends ClioDocument
