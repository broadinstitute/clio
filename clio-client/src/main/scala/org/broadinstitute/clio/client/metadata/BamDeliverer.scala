package org.broadinstitute.clio.client.metadata

import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{IoOp, WriteOp}
import org.broadinstitute.clio.transfer.model.bam.{BamExtensions, BamMetadata}

case class BamDeliverer() extends MetadataMover[BamMetadata] {
  override protected def moveMetadata(
    src: BamMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (BamMetadata, Iterable[IoOp]) = {
    val movedBam = src.bamPath.map(
      MetadataMover
        .buildFilePath(_, destination, newBasename.map(_ + BamExtensions.BamExtension))
    )

    val movedBamsDest = src.copy(
      bamPath = movedBam,
      baiPath = movedBam.map(
        bamUri => URI.create(s"$bamUri${BamExtensions.BaiExtensionAddition}")
      )
    )

    val writeMd5Op = for {
      md5 <- src.bamMd5
      bam <- movedBam
    } yield {
      WriteOp(md5.name, URI.create(s"$bam${BamExtensions.Md5ExtensionAddition}"))
    }

    val ops = Iterable(
      extractMoves(src, movedBamsDest, _.bamPath),
      extractMoves(src, movedBamsDest, _.baiPath),
      writeMd5Op.toIterable
    ).flatten

    (movedBamsDest, ops)
  }
}
