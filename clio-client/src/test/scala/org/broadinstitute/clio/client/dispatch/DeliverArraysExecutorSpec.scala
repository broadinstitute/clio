package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.stream.scaladsl.Sink
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{CopyOp, MoveOp}
import org.broadinstitute.clio.transfer.model.Metadata
import org.broadinstitute.clio.transfer.model.arrays.{
  ArraysExtensions,
  ArraysKey,
  ArraysMetadata
}
import org.broadinstitute.clio.util.model.Location
import org.scalamock.scalatest.AsyncMockFactory

class DeliverArraysExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "DeliverArraysExecutor"

  private val theKey = ArraysKey(
    chipwellBarcode = Symbol("abcdefg"),
    version = 1,
    location = Location.GCP
  )
  private val metadata = ArraysMetadata(
    vcfPath = Some(URI.create(s"gs://bucket/the-vcf${ArraysExtensions.VcfGzExtension}")),
    vcfIndexPath =
      Some(URI.create(s"gs://bucket/the-vcf${ArraysExtensions.VcfGzTbiExtension}")),
    gtcPath = Some(URI.create(s"gs://bucket/the-gct${ArraysExtensions.GtcExtension}")),
    grnIdatPath =
      Some(URI.create(s"gs://bucket/the-grn${ArraysExtensions.GrnIdatExtension}")),
    redIdatPath =
      Some(URI.create(s"gs://bucket/the-red${ArraysExtensions.RedIdatExtension}")),
    refFastaPath =
      Some(URI.create(s"gs://bucket/the-ref${ArraysExtensions.FastaExtension}")),
    refFastaIndexPath =
      Some(URI.create(s"gs://bucket/the-ref${ArraysExtensions.FastaFaiExtension}")),
    refDictPath =
      Some(URI.create(s"gs://bucket/the-ref${ArraysExtensions.DictExtension}"))
  )
  private val workspaceName = "the-workspace"
  private val destination = URI.create("gs://the-destination/")

  it should "add IO ops to copy the idats, and add workspace name" in {
    val executor =
      new DeliverArraysExecutor(DeliverArrays(theKey, workspaceName, destination))

    executor.buildMove(metadata).runWith(Sink.head).map {
      case (newMetadata, ops) =>
        val movedGrn = metadata.grnIdatPath.map(
          Metadata.buildFilePath(
            _,
            destination.resolve(DeliverArraysExecutor.IdatsDir),
            ArraysExtensions.IdatExtension
          )
        )
        val movedRed = metadata.redIdatPath.map(
          Metadata.buildFilePath(
            _,
            destination.resolve(DeliverArraysExecutor.IdatsDir),
            ArraysExtensions.IdatExtension
          )
        )
        val movedVcf = metadata.vcfPath.map(
          Metadata.buildFilePath(
            _,
            destination,
            ArraysExtensions.VcfGzExtension
          )
        )
        val movedVcfIndex = metadata.vcfIndexPath.map(
          Metadata.buildFilePath(
            _,
            destination,
            ArraysExtensions.VcfGzTbiExtension
          )
        )
        val movedGtc = metadata.gtcPath.map(
          Metadata.buildFilePath(
            _,
            destination,
            ArraysExtensions.GtcExtension
          )
        )
        val grnMove = metadata.grnIdatPath.zip(movedGrn).map {
          case (src, dest) => CopyOp(src, dest)
        }
        val redMove = metadata.redIdatPath.zip(movedRed).map {
          case (src, dest) => CopyOp(src, dest)
        }
        val vcfMove = metadata.vcfPath.zip(movedVcf).map {
          case (src, dest) => MoveOp(src, dest)
        }
        val vcfIndexMove = metadata.vcfIndexPath.zip(movedVcfIndex).map {
          case (src, dest) => MoveOp(src, dest)
        }
        val gtcMove = metadata.gtcPath.zip(movedGtc).map {
          case (src, dest) => MoveOp(src, dest)
        }

        newMetadata.workspaceName should be(Some(workspaceName))
        newMetadata.grnIdatPath should be(movedGrn)
        newMetadata.redIdatPath should be(movedRed)
        newMetadata.vcfPath should be(movedVcf)
        newMetadata.vcfIndexPath should be(movedVcfIndex)
        newMetadata.gtcPath should be(movedGtc)

        ops should contain allElementsOf Seq
          .concat(grnMove, redMove, vcfMove, vcfIndexMove, gtcMove)
    }
  }

  it should "fail if no red idat is registered to a document" in {

    val executor =
      new DeliverArraysExecutor(DeliverArrays(theKey, workspaceName, destination))

    recoverToSucceededIf[IllegalStateException] {
      executor.buildMove(metadata.copy(grnIdatPath = None)).runWith(Sink.ignore)
    }
  }

  it should "fail if no green idat is registered to a document" in {

    val executor =
      new DeliverArraysExecutor(DeliverArrays(theKey, workspaceName, destination))

    recoverToSucceededIf[IllegalStateException] {
      executor.buildMove(metadata.copy(redIdatPath = None)).runWith(Sink.ignore)
    }
  }
}
