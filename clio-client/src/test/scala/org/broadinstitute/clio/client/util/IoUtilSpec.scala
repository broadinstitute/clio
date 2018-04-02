package org.broadinstitute.clio.client.util

import org.scalatest.{FlatSpec, Matchers}

//test the non-gs portions of io util
class IoUtilSpec extends FlatSpec with Matchers with TestData {
  behavior of "IoUtil"

  it should "read a metadata file from file location" in {
    IoUtil.readMetadata(metadataFileLocation) should startWith("{")
  }
}
