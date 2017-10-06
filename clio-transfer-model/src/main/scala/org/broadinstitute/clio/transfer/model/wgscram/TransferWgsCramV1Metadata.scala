package org.broadinstitute.clio.transfer.model.wgscram

import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.DocumentStatus

case class TransferWgsCramV1Metadata(documentStatus: Option[DocumentStatus] = None,
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
                                    ) extends TransferMetadata[TransferWgsCramV1Metadata] {

  override def pathsToMove: Seq[String] = (Seq(cramPath, craiPath, cramMd5Path, logPath, fingerprintPath, workflowJsonPath, optionsJsonPath, wdlPath) ++ analysisFiles).flatten

  override def moveAllInto(destination: String): TransferWgsCramV1Metadata = {
    if (!destination.endsWith("/")) {
      throw new Exception("Arguments to `moveAllInto` must end with '/'")
    }

    this.copy(
      cramPath = cramPath.map(moveInto(_, destination)),
      craiPath = craiPath.map(moveInto(_, destination)),
      cramMd5Path = cramMd5Path.map(moveInto(_, destination)),
      fingerprintPath = fingerprintPath.map(moveInto(_, destination)),
      workflowJsonPath = workflowJsonPath.map(moveInto(_, destination)),
      optionsJsonPath = optionsJsonPath.map(moveInto(_, destination)),
      wdlPath = wdlPath.map(moveInto(_, destination)),
      analysisFiles = analysisFiles.map(moveInto(_, destination))
    )
  }

  override def setSinglePath(destination: String): TransferWgsCramV1Metadata = {
    throw new Exception(
      s"`setSinglePath` not implemented for $getClass"
    )
  }
}
