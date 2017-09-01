package org.broadinstitute.client.util

import org.broadinstitute.clio.client.util.IoUtil
import org.scalatest.{AsyncFlatSpec, Matchers}

//test the non-gs portions of io util
class IoUtilSpec extends AsyncFlatSpec with Matchers with TestData {
  behavior of "IoUtil"

  it should "read a metadata file from file location" in {
    IoUtil.readMetadata(metadataFileLocation) should startWith("{")
  }

}
