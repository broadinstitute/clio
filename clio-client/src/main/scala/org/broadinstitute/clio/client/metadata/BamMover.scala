package org.broadinstitute.clio.client.metadata
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.MoveOp
import org.broadinstitute.clio.transfer.model.bam.{BamExtensions, BamMetadata}

class BamMover extends MetadataMover[BamMetadata] {
  override protected def moveMetadata(
    src: BamMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (BamMetadata, Iterable[MoveOp]) = {
    import MetadataMover.buildFilePath

    val movedBam = src.bamPath.map(
      buildFilePath(_, destination, newBasename.map(_ + BamExtensions.BamExtension))
    )

    val dest = src.copy(
      bamPath = movedBam,
      baiPath = movedBam.map(BamExtensions.replaceBamExtensionWithBaiExtension)
    )

    val ops = Seq[BamMetadata => Iterable[URI]](
      _.bamPath,
      _.baiPath
    ).flatMap(extractMoves(src, dest, _))

    (dest, ops)
  }
}
