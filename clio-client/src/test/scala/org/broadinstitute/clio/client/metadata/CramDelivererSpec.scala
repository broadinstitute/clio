package org.broadinstitute.clio.client.metadata

import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{MoveOp, WriteOp}
import org.broadinstitute.clio.transfer.model.wgscram.{CramExtensions, CramMetadata}
import org.scalatest.{FlatSpec, Matchers}

class CramDelivererSpec extends FlatSpec with Matchers {
  behavior of "CramDeliverer"

  private val cramName = s"the-cram${CramExtensions.CramExtension}"
  private val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"

  private val cramPath = URI.create(s"gs://bucket/$cramName")
  private val craiPath = URI.create(s"gs://bucket/$craiName")
  private val cramMd5 = Symbol("abcdefg")

  private val metadata =
    CramMetadata(
      cramPath = Some(cramPath),
      craiPath = Some(craiPath),
      cramMd5 = Some(cramMd5)
    )
  private val destination = URI.create("gs://the-destination/")

  private val deliverer = new CramDeliverer

  it should "generate ops to move the cram & crai, and write the cram md5" in {
    val (delivered, ops) = deliverer.moveInto(metadata, destination)

    delivered.cramPath should be(Some(destination.resolve(cramName)))
    delivered.craiPath should be(Some(destination.resolve(craiName)))

    ops should contain theSameElementsAs Seq(
      MoveOp(cramPath, destination.resolve(cramName)),
      MoveOp(craiPath, destination.resolve(craiName)),
      WriteOp(
        cramMd5.name,
        destination.resolve(s"$cramName${CramExtensions.Md5ExtensionAddition}")
      )
    )
  }

  it should "use the new basename for the md5 file, if given" in {
    val basename = "the-new-basename"

    val (delivered, ops) = deliverer.moveInto(metadata, destination, Some(basename))

    val cramName = s"$basename${CramExtensions.CramExtension}"
    val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"

    delivered.cramPath should be(Some(destination.resolve(cramName)))
    delivered.craiPath should be(Some(destination.resolve(craiName)))

    ops should contain theSameElementsAs Seq(
      MoveOp(cramPath, destination.resolve(cramName)),
      MoveOp(craiPath, destination.resolve(craiName)),
      WriteOp(
        cramMd5.name,
        destination.resolve(s"$cramName${CramExtensions.Md5ExtensionAddition}")
      )
    )
  }
}
