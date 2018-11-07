package org.broadinstitute.clio.client.metadata

import java.net.URI

import org.broadinstitute.clio.client.dispatch.MoveExecutor.{MoveOp, WriteOp}
import org.broadinstitute.clio.transfer.model.cram.{CramExtensions, CramMetadata}
import org.broadinstitute.clio.util.model.RegulatoryDesignation
import org.scalatest.{FlatSpec, Ignore, Matchers}

@Ignore
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
  private val fingerprintingSummaryMetricsName = "the-fingerprintSummaryMetricsName"
  private val fingerprintingDetailMetricsName = "the-fingerprintDetailMetricsName"

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
      regulatoryDesignation = Some(RegulatoryDesignation.ResearchOnly),
      fingerprintingSummaryMetricsPath = Some(fingerprintingSummaryMetricsPath),
      fingerprintingDetailMetricsPath = Some(fingerprintingDetailMetricsPath)
    )
  private val destination = URI.create("gs://the-destination/")

  private val delivererWithMetrics = CramDeliverer(deliverSampleMetrics = true)

  private val delivererNoMetrics = CramDeliverer(deliverSampleMetrics = false)

  it should "generate ops to move the cram & crai, write the cram md5 and copy metrics files" in {
    val (delivered, ops) = delivererWithMetrics.moveInto(metadata, destination)

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
      Some(fingerprintingDetailMetricsPath)
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
      WriteOp(
        cramMd5.name,
        destination.resolve(s"$cramName${CramExtensions.Md5ExtensionAddition}")
      )
    )

    ops should not contain MoveOp(
      fingerprintingDetailMetricsPath,
      destination.resolve(fingerprintingDetailMetricsName)
    )
  }

  it should "use the new basename for the md5 file, if given, and copy metrics files" in {
    val basename = "the-new-basename"

    val (delivered, ops) =
      delivererWithMetrics.moveInto(metadata, destination, Some(basename))

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
      )
    )
  }
  it should "generate ops to move the cram & crai, and write the cram md5" in {
    val (delivered, ops) = delivererNoMetrics.moveInto(metadata, destination)

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

    val (delivered, ops) =
      delivererNoMetrics.moveInto(metadata, destination, Some(basename))

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
