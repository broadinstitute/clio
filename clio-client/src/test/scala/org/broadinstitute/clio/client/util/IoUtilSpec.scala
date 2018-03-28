package org.broadinstitute.clio.client.util

import java.net.URI

import org.broadinstitute.clio.client.util.IoUtil.GsUtil
import org.scalatest.{FlatSpec, Matchers}

//test the non-gs portions of io util
class IoUtilSpec extends FlatSpec with Matchers with TestData {
  behavior of "IoUtil"

  private val uri = URI.create(
    "gs://broad-gotc-dev-storage/pipeline/C1963/CHMI_CHMI3_Nex1/v1/id.txt"
  )

  it should "read a metadata file from file location" in {
    IoUtil.readMetadata(metadataFileLocation) should startWith("{")
  }
}
