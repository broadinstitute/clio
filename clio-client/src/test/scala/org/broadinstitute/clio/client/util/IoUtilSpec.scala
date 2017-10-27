package org.broadinstitute.clio.client.util

import org.scalatest.{FlatSpec, Matchers}

//test the non-gs portions of io util
class IoUtilSpec extends FlatSpec with Matchers with TestData {
  behavior of "IoUtil"

  it should "read a metadata file from file location" in {
    IoUtil.readMetadata(metadataFileLocation) should startWith("{")
  }

  it should "Compute the correct md5 hash of a file and return just the hash" in {
    // This command also works for local files, so we're using that to check the parsing of the output
    // NOTE: we use equal() instead of be() here because be() has special behavior for symbols.
    IoUtil.getMd5HashOfGoogleObject(metadataFileLocation) should equal(
      Symbol("240a1a8639f7142c871a659a85f69cce")
    )
  }

}
