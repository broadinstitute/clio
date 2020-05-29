package org.broadinstitute.clio.client.metadata

import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{CopyOp, MoveOp}
import org.broadinstitute.clio.transfer.model.arrays.{ArraysExtensions, ArraysMetadata}
import org.scalatest.{FlatSpec, Matchers}

class ArrayUndelivererSpec extends FlatSpec with Matchers {
  behavior of "ArrayUndeliverer"

  private val vcfName = s"the-vcf${ArraysExtensions.VcfGzExtension}"
  private val vcfIndexName = s"$vcfName${ArraysExtensions.VcfGzTbiExtension}"
  private val gtcName = s"the-gtc${ArraysExtensions.GtcExtension}"
  private val grnName = s"the-grn${ArraysExtensions.GrnIdatExtension}"
  private val redName = s"the-red${ArraysExtensions.RedIdatExtension}"

  private val vcfPath = URI.create(s"gs://bucket/$vcfName")
  private val vcfIndexPath = URI.create(s"gs://bucket/$vcfIndexName")
  private val gtcPath = URI.create(s"gs://bucket/$gtcName")
  private val grnPath = URI.create(s"gs://bucket/$grnName")
  private val redPath = URI.create(s"gs://bucket/$redName")

  private val workspaceName = "workspace"
  private val billingProject = "billingProject"

  private val metadata = ArraysMetadata(
    vcfPath = Some(vcfPath),
    vcfIndexPath = Some(vcfIndexPath),
    gtcPath = Some(gtcPath),
    grnIdatPath = Some(grnPath),
    redIdatPath = Some(redPath),
    workspaceName = Some(workspaceName),
    billingProject = Some(billingProject)
  )
  private val destination = URI.create("gs://the-destination/")

  private val undeliverer = new ArrayUndeliverer

  it should "generate ops to move the vcf, index, and gtc + copy the idats" in {
    val (delivered, ops) = undeliverer.moveInto(metadata, destination)

    val idatDestination = destination.resolve(ArrayDeliverer.IdatsDir)

    delivered.vcfPath should be(Some(destination.resolve(vcfName)))
    delivered.vcfIndexPath should be(Some(destination.resolve(vcfIndexName)))
    delivered.gtcPath should be(Some(destination.resolve(gtcName)))
    delivered.grnIdatPath should be(Some(idatDestination.resolve(grnName)))
    delivered.redIdatPath should be(Some(idatDestination.resolve(redName)))

    ops should contain theSameElementsAs Seq(
      MoveOp(vcfPath, destination.resolve(vcfName)),
      MoveOp(vcfIndexPath, destination.resolve(vcfIndexName)),
      MoveOp(gtcPath, destination.resolve(gtcName)),
      CopyOp(grnPath, idatDestination.resolve(grnName)),
      CopyOp(redPath, idatDestination.resolve(redName))
    )
  }

  it should "move the idats when undelivering from an existing workspace" in {
    val deliveredMetadata = metadata.copy(workspaceName = Some("firecloud-workspace"))
    val (delivered, ops) = undeliverer.moveInto(deliveredMetadata, destination)

    val idatDestination = destination.resolve(ArrayDeliverer.IdatsDir)

    delivered.vcfPath should be(Some(destination.resolve(vcfName)))
    delivered.vcfIndexPath should be(Some(destination.resolve(vcfIndexName)))
    delivered.gtcPath should be(Some(destination.resolve(gtcName)))
    delivered.grnIdatPath should be(Some(idatDestination.resolve(grnName)))
    delivered.redIdatPath should be(Some(idatDestination.resolve(redName)))

    ops should contain theSameElementsAs Seq(
      MoveOp(vcfPath, destination.resolve(vcfName)),
      MoveOp(vcfIndexPath, destination.resolve(vcfIndexName)),
      MoveOp(gtcPath, destination.resolve(gtcName)),
      MoveOp(grnPath, idatDestination.resolve(grnName)),
      MoveOp(redPath, idatDestination.resolve(redName))
    )
  }
}
