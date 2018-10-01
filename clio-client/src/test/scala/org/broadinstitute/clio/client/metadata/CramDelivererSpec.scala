package org.broadinstitute.clio.client.metadata

import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{MoveOp, WriteOp}
import org.broadinstitute.clio.transfer.model.wgscram.{CramExtensions, CramMetadata}
import org.broadinstitute.clio.util.model.RegulatoryDesignation
import org.scalatest.{FlatSpec, Matchers}

class CramDelivererSpec extends FlatSpec with Matchers {
  behavior of "CramDeliverer"

  private val cramName = s"the-cram${CramExtensions.CramExtension}"
  private val craiName = s"$cramName${CramExtensions.CraiExtensionAddition}"

  private val cramPath = URI.create(s"gs://bucket/$cramName")
  private val craiPath = URI.create(s"gs://bucket/$craiName")
  private val cramMd5 = Symbol("abcdefg")

  private val preAdapterSummaryMetricsName = "the-preAdapterSummaryMetricsName"
  private val preAdapterDetailMetricsName = "the-preAdapterDetailMetricsName"
  private val alignmentSummaryMetricsName = "the-alignmentSummaryMetricsName"
  private val duplicateMetricsName = "the-duplicateMetricsName"
  private val fingerprintingSummaryMetricsName = "the-fingerprintingSummaryMetricsName"
  private val fingerprintingDetailMetricsName = "the-fingerprintingDetailMetricsName"

  private val preAdapterSummaryMetricsPath =
    URI.create(s"gs://bucket/$preAdapterSummaryMetricsName")
  private val preAdapterDetailMetricsPath =
    URI.create(s"gs://bucket/$preAdapterDetailMetricsName")
  private val alignmentSummaryMetricsPath =
    URI.create(s"gs://bucket/$alignmentSummaryMetricsName")
  private val duplicateMetricsPath = URI.create(s"gs://bucket/$duplicateMetricsName")
  private val fingerprintingSummaryMetricsPath =
    URI.create(s"gs://bucket/$fingerprintingSummaryMetricsName")
  private val fingerprintingDetailMetricsPath =
    URI.create(s"gs://bucket/$fingerprintingDetailMetricsName")

  private val metadata =
    CramMetadata(
      cramPath = Some(cramPath),
      craiPath = Some(craiPath),
      cramMd5 = Some(cramMd5),
      preAdapterSummaryMetricsPath = Some(preAdapterSummaryMetricsPath),
      preAdapterDetailMetricsPath = Some(preAdapterDetailMetricsPath),
      alignmentSummaryMetricsPath = Some(alignmentSummaryMetricsPath),
      duplicateMetricsPath = Some(duplicateMetricsPath),
      fingerprintingSummaryMetricsPath = Some(fingerprintingSummaryMetricsPath),
      fingerprintingDetailMetricsPath = Some(fingerprintingDetailMetricsPath),
      regulatoryDesignation = Some(RegulatoryDesignation.ResearchOnly)
    )
  private val destination = URI.create("gs://the-destination/")

  private val deliverer = new CramDeliverer

  it should "generate ops to move the cram & crai, and write the cram md5" in {
    val (delivered, ops) = deliverer.moveInto(metadata, destination)

    delivered.cramPath should be(Some(destination.resolve(cramName)))
    delivered.craiPath should be(Some(destination.resolve(craiName)))
    delivered.preAdapterSummaryMetricsPath should be(
      Some(destination.resolve(preAdapterSummaryMetricsName))
    )
    delivered.preAdapterDetailMetricsPath should be(
      Some(destination.resolve(preAdapterDetailMetricsName))
    )
    delivered.alignmentSummaryMetricsPath should be(
      Some(destination.resolve(alignmentSummaryMetricsName))
    )
    delivered.duplicateMetricsPath should be(
      Some(destination.resolve(duplicateMetricsName))
    )
    delivered.fingerprintingSummaryMetricsPath should be(
      Some(destination.resolve(fingerprintingSummaryMetricsName))
    )
    delivered.fingerprintingDetailMetricsPath should be(
      Some(destination.resolve(fingerprintingDetailMetricsName))
    )

    ops should contain theSameElementsAs Seq(
      MoveOp(cramPath, destination.resolve(cramName)),
      MoveOp(craiPath, destination.resolve(craiName)),
      MoveOp(
        preAdapterSummaryMetricsPath,
        destination.resolve(preAdapterSummaryMetricsName)
      ),
      MoveOp(
        preAdapterDetailMetricsPath,
        destination.resolve(preAdapterDetailMetricsName)
      ),
      MoveOp(
        alignmentSummaryMetricsPath,
        destination.resolve(alignmentSummaryMetricsName)
      ),
      MoveOp(duplicateMetricsPath, destination.resolve(duplicateMetricsName)),
      MoveOp(
        fingerprintingSummaryMetricsPath,
        destination.resolve(fingerprintingSummaryMetricsName)
      ),
      MoveOp(
        fingerprintingDetailMetricsPath,
        destination.resolve(fingerprintingDetailMetricsName)
      ),
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
      ),
      MoveOp(
        preAdapterSummaryMetricsPath,
        destination.resolve(preAdapterSummaryMetricsName)
      ),
      MoveOp(
        preAdapterDetailMetricsPath,
        destination.resolve(preAdapterDetailMetricsName)
      ),
      MoveOp(
        alignmentSummaryMetricsPath,
        destination.resolve(alignmentSummaryMetricsName)
      ),
      MoveOp(duplicateMetricsPath, destination.resolve(duplicateMetricsName)),
      MoveOp(
        fingerprintingSummaryMetricsPath,
        destination.resolve(fingerprintingSummaryMetricsName)
      ),
      MoveOp(
        fingerprintingDetailMetricsPath,
        destination.resolve(fingerprintingDetailMetricsName)
      ),
    )
  }
}
