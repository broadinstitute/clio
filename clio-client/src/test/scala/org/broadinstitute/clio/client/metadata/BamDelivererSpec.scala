package org.broadinstitute.clio.client.metadata

import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{MoveOp, WriteOp}
import org.broadinstitute.clio.transfer.model.bam.{BamExtensions, BamMetadata}
import org.scalatest.{FlatSpec, Ignore, Matchers}

@Ignore
class BamDelivererSpec extends FlatSpec with Matchers {
  behavior of "BamDeliverer"

  private val bamName = s"the-bam${BamExtensions.BamExtension}"
  private val baiName = s"$bamName${BamExtensions.BaiExtensionAddition}"

  private val bamPath = URI.create(s"gs://bucket/$bamName")
  private val baiPath = URI.create(s"gs://bucket/$baiName")
  private val bamMd5 = Symbol("abcdefg")

  private val metadata =
    BamMetadata(
      bamPath = Some(bamPath),
      baiPath = Some(baiPath),
      bamMd5 = Some(bamMd5)
    )
  private val destination = URI.create("gs://the-destination/")

  private val deliverer = BamDeliverer()

  it should "generate ops to move the bam & bai, and write the bam md5" in {
    val (delivered, ops) = deliverer.moveInto(metadata, destination)

    delivered.bamPath should be(Some(destination.resolve(bamName)))
    delivered.baiPath should be(Some(destination.resolve(baiName)))

    ops should contain theSameElementsAs Seq(
      MoveOp(bamPath, destination.resolve(bamName)),
      MoveOp(baiPath, destination.resolve(baiName)),
      WriteOp(
        bamMd5.name,
        destination.resolve(s"$bamName${BamExtensions.Md5ExtensionAddition}")
      )
    )
  }

  it should "use the new basename for the md5 file, if given" in {
    val basename = "the-new-basename"

    val (delivered, ops) =
      deliverer.moveInto(metadata, destination, Some(basename))

    val bamName = s"$basename${BamExtensions.BamExtension}"
    val baiName = s"$bamName${BamExtensions.BaiExtensionAddition}"

    delivered.bamPath should be(Some(destination.resolve(bamName)))
    delivered.baiPath should be(Some(destination.resolve(baiName)))

    ops should contain theSameElementsAs Seq(
      MoveOp(bamPath, destination.resolve(bamName)),
      MoveOp(baiPath, destination.resolve(baiName)),
      WriteOp(
        bamMd5.name,
        destination.resolve(s"$bamName${BamExtensions.Md5ExtensionAddition}")
      )
    )
  }
}
