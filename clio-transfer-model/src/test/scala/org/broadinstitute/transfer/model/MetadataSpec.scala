package org.broadinstitute.transfer.model

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.Metadata
import org.broadinstitute.clio.transfer.model.gvcf.GvcfMetadata
import org.broadinstitute.clio.transfer.model.ubam.UbamMetadata
import org.broadinstitute.clio.util.model.DocumentStatus
import org.scalatest.{FlatSpec, Matchers}

class MetadataSpec extends FlatSpec with Matchers {
  behavior of "Metadata"

  it should "extract paths out of metadata with one path" in {
    Metadata.extractPaths(
      UbamMetadata(ubamPath = Some(URI.create("gs://the-ubam")), ubamSize = Some(1000L))
    ) should contain theSameElementsAs Map("ubamPath" -> URI.create("gs://the-ubam"))
  }

  it should "extract paths out of metadata with multiple paths" in {
    Metadata.extractPaths(
      GvcfMetadata(
        analysisDate = Some(OffsetDateTime.now()),
        gvcfPath = Some(URI.create("gs://the-gvcf")),
        gvcfDetailMetricsPath = Some(URI.create("gs://the-metrics")),
        documentStatus = Some(DocumentStatus.Normal)
      )
    ) should contain theSameElementsAs Map(
      "gvcfPath" -> URI.create("gs://the-gvcf"),
      "gvcfDetailMetricsPath" -> URI.create("gs://the-metrics")
    )
  }

  it should "extract an empty map out of metadata with no paths" in {
    Metadata.extractPaths(UbamMetadata(sampleId = Some(Symbol("abcdefg")))) shouldBe empty
  }

}
