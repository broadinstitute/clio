package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.stream.scaladsl.Sink
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeliverArrays
import org.broadinstitute.clio.client.dispatch.MoveExecutor.CopyOp
import org.broadinstitute.clio.client.util.IoUtil
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
      Some(URI.create(s"gs://bucket/the-grn${ArraysExtensions.IdatExtension}")),
    redIdatPath =
      Some(URI.create(s"gs://bucket/the-red${ArraysExtensions.IdatExtension}"))
  )
  private val workspaceName = "the-workspace"
  private val destination = URI.create("gs://the-destination/")

  it should "add IO ops to copy the idats, and add workspace name" in {
    val ioUtil = mock[IoUtil]
    Seq.concat(metadata.vcfPath, metadata.vcfIndexPath, metadata.gtcPath).foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
    }

    val executor =
      new DeliverArraysExecutor(DeliverArrays(theKey, workspaceName, destination))

    executor.buildMove(metadata, ioUtil).runWith(Sink.head).map {
      case (newMetadata, ops) =>
        val movedGrn = metadata.grnIdatPath.map(
          Metadata.findNewPathForMove(_, destination, ArraysExtensions.IdatExtension)
        )
        val movedRed = metadata.redIdatPath.map(
          Metadata.findNewPathForMove(_, destination, ArraysExtensions.IdatExtension)
        )

        val grnMove = metadata.grnIdatPath.zip(movedGrn).map {
          case (src, dest) => CopyOp(src, dest)
        }
        val redMove = metadata.redIdatPath.zip(movedRed).map {
          case (src, dest) => CopyOp(src, dest)
        }

        newMetadata.workspaceName should be(Some(workspaceName))
        newMetadata.grnIdatPath should be(movedGrn)
        newMetadata.redIdatPath should be(movedRed)

        ops should contain allElementsOf Seq.concat(grnMove, redMove)
    }
  }

  it should "fail if no red idat is registered to a document" in {
    val ioUtil = mock[IoUtil]
    Seq.concat(metadata.vcfPath, metadata.vcfIndexPath, metadata.gtcPath).foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
    }

    val executor =
      new DeliverArraysExecutor(DeliverArrays(theKey, workspaceName, destination))

    recoverToSucceededIf[IllegalStateException] {
      executor.buildMove(metadata.copy(grnIdatPath = None), ioUtil).runWith(Sink.ignore)
    }
  }

  it should "fail if no green idat is registered to a document" in {
    val ioUtil = mock[IoUtil]
    Seq.concat(metadata.vcfPath, metadata.vcfIndexPath, metadata.gtcPath).foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
    }

    val executor =
      new DeliverArraysExecutor(DeliverArrays(theKey, workspaceName, destination))

    recoverToSucceededIf[IllegalStateException] {
      executor.buildMove(metadata.copy(redIdatPath = None), ioUtil).runWith(Sink.ignore)
    }
  }
}
