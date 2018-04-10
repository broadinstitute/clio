package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.stream.scaladsl.Sink
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeliverWgsCram
import org.broadinstitute.clio.client.dispatch.MoveExecutor.WriteOp
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.transfer.model.wgscram.{
  WgsCramExtensions,
  WgsCramKey,
  WgsCramMetadata
}
import org.broadinstitute.clio.util.model.Location
import org.scalamock.scalatest.AsyncMockFactory

class DeliverWgsCramExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "DeliverWgsCramExecutor"

  private val theKey = WgsCramKey(
    location = Location.GCP,
    project = "the-project",
    sampleAlias = "the-sample",
    version = 1
  )
  private val cramPath =
    URI.create(s"gs://bucket/the-cram${WgsCramExtensions.CramExtension}")
  private val cramMd5 = Symbol("abcdefg")
  private val metadata =
    WgsCramMetadata(cramPath = Some(cramPath), cramMd5 = Some(cramMd5))
  private val workspaceName = "the-workspace"
  private val destination = URI.create("gs://the-destination/")

  it should "add an IO op to write the cram md5, and add workspace name" in {
    val ioUtil = mock[IoUtil]
    (ioUtil.isGoogleObject _).expects(cramPath).returning(true)

    val executor =
      new DeliverWgsCramExecutor(DeliverWgsCram(theKey, workspaceName, destination))

    executor.buildMove(metadata, ioUtil).runWith(Sink.head).map {
      case (newMetadata, ops) =>
        newMetadata.workspaceName should be(Some(workspaceName))
        ops should contain(
          WriteOp(
            cramMd5.name,
            destination.resolve(s"the-cram${WgsCramExtensions.Md5Extension}")
          )
        )
    }
  }

  it should "use the new basename for the md5 file, if given" in {
    val basename = "the-new-basename"

    val ioUtil = mock[IoUtil]
    (ioUtil.isGoogleObject _).expects(cramPath).returning(true)

    val executor =
      new DeliverWgsCramExecutor(
        DeliverWgsCram(theKey, workspaceName, destination, Some(basename))
      )

    executor.buildMove(metadata, ioUtil).runWith(Sink.head).map {
      case (newMetadata, ops) =>
        newMetadata.workspaceName should be(Some(workspaceName))
        ops should contain(
          WriteOp(
            cramMd5.name,
            destination.resolve(s"$basename${WgsCramExtensions.Md5Extension}")
          )
        )
    }
  }

  it should "fail if no cram is registered to a document" in {
    val executor =
      new DeliverWgsCramExecutor(DeliverWgsCram(theKey, workspaceName, destination))

    recoverToSucceededIf[IllegalStateException] {
      executor
        .buildMove(metadata.copy(cramPath = None), stub[IoUtil])
        .runWith(Sink.ignore)
    }
  }

  it should "fail if no cram md5 is registered to a document" in {
    val ioUtil = mock[IoUtil]
    (ioUtil.isGoogleObject _).expects(cramPath).returning(true)

    val executor =
      new DeliverWgsCramExecutor(DeliverWgsCram(theKey, workspaceName, destination))

    recoverToSucceededIf[IllegalStateException] {
      executor.buildMove(metadata.copy(cramMd5 = None), ioUtil).runWith(Sink.ignore)
    }
  }
}
