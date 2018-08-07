package org.broadinstitute.clio.client.metadata
import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{IoOp, WriteOp}
import org.broadinstitute.clio.transfer.model.wgscram.{CramExtensions, CramMetadata}

class CramDeliverer extends MetadataMover[CramMetadata] {
  override protected def moveMetadata(
    src: CramMetadata,
    destination: URI,
    newBasename: Option[String]
  ): (CramMetadata, Iterable[IoOp]) = {
    val movedCram = src.cramPath.map(
      MetadataMover
        .buildFilePath(_, destination, newBasename.map(_ + CramExtensions.CramExtension))
    )

    val dest = src.copy(
      cramPath = movedCram,
      craiPath = movedCram.map(
        cramUri => URI.create(s"$cramUri${CramExtensions.CraiExtensionAddition}")
      )
    )

    val writeMd5Op = for {
      md5 <- src.cramMd5
      cram <- movedCram
    } yield {
      WriteOp(md5.name, URI.create(s"$cram${CramExtensions.Md5ExtensionAddition}"))
    }

    val ops = Iterable(
      extractMoves(src, dest, _.cramPath),
      extractMoves(src, dest, _.craiPath),
      writeMd5Op.toIterable
    ).flatten

    (dest, ops)
  }
}
