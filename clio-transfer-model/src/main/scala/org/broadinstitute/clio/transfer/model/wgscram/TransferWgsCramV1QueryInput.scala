package org.broadinstitute.clio.transfer.model.wgscram

import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

case class TransferWgsCramV1QueryInput(
                                        documentStatus: Option[DocumentStatus] = None,
                                        location: Option[Location] = None,
                                        project: Option[String] = None,
                                        sampleAlias: Option[String] = None,
                                        version: Option[Int],
                                        pipelineVersion: Option[Long] = None,
                                        workflowStartDate: Option[OffsetDateTime] = None,
                                        workflowEndDate: Option[OffsetDateTime] = None,
                                        cramMd5: Option[String] = None,
                                        cramSize: Option[Long] = None,
                                        cramPath: Option[String] = None,
                                        craiPath: Option[String] = None,
                                        cramMd5Path: Option[String] = None,
                                        logPath: Option[String] = None,
                                        fingerprintPath: Option[String] = None,
                                        cromwellId: Option[String] = None,
                                        workflowJsonPath: Option[String] = None,
                                        optionsJsonPath: Option[String] = None,
                                        wdlPath: Option[String] = None,
                                        readgroupMd5: Option[String] = None,
                                        analysisFiles: List[String] = List(),
                                        notes: Option[String] = None
                                      )
