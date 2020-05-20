package org.broadinstitute.clio.transfer.model.bam

import java.net.URI

import org.broadinstitute.clio.transfer.model.{DeliverableMetadata, Metadata}
import org.broadinstitute.clio.util.model.DocumentStatus

case class BamMetadata(
  documentStatus: Option[DocumentStatus] = None,
  bamMd5: Option[Symbol] = None,
  bamSize: Option[Long] = None,
  bamPath: Option[URI] = None,
  baiPath: Option[URI] = None,
  workspaceName: Option[String] = None,
  billingProject: Option[String] = None,
  notes: Option[String] = None,
) extends Metadata[BamMetadata]
    with DeliverableMetadata[BamMetadata] {

  override def pathsToDelete: Seq[URI] =
    Seq.concat(
      bamPath,
      baiPath,
      // Delete the bamPath.md5 file only if a workspaceName is defined otherwise there will be no md5
      // (foo.bam.md5 where foo.bam is bamPath)
      workspaceName.flatMap(
        _ =>
          bamPath.map { cp =>
            URI.create(s"$cp${BamExtensions.Md5ExtensionAddition}")
        }
      )
    )

  override def changeStatus(
    documentStatus: DocumentStatus,
    changeNote: String
  ): BamMetadata =
    this.copy(
      documentStatus = Some(documentStatus),
      notes = appendNote(changeNote)
    )

  override def withWorkspace(name: String, billingProject: String): BamMetadata = {
    this.copy(
      workspaceName = Some(name),
      billingProject = Some(billingProject)
    )
  }

  override def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): BamMetadata =
    this.copy(
      documentStatus = documentStatus
    )

  val sampleLevelMetrics = Iterable.empty
}
