package org.broadinstitute.clio.client.util

import java.net.URI

import org.broadinstitute.clio.client.util.IoUtil.GsUtil
import org.scalatest.{FlatSpec, Matchers}

//test the non-gs portions of io util
class IoUtilSpec extends FlatSpec with Matchers with TestData {
  behavior of "IoUtil"

  val uri = URI.create(
    "gs://broad-gotc-dev-storage/pipeline/C1963/CHMI_CHMI3_Nex1/v1/id.txt"
  )

  it should "read a metadata file from file location" in {
    IoUtil.readMetadata(metadataFileLocation) should startWith("{")
  }

  it should "parse the md5 hash out of 'gsutil hash' output" in {
    val expectedHash = Symbol("998e42da13d7bd619f798baf8ea08a13")
    val mockGsUtil = new GsUtil(None) {
      override def hash(path: String): String =
        s"""Hashes [hex] for pipeline/C1963/CHMI_CHMI3_Nex1/v1/id.txt:
           |        Hash (crc32c):          392eca9a
           |        Hash (md5):             ${expectedHash.name}
         """.stripMargin
    }

    new IoUtil { override val gsUtil: GsUtil = mockGsUtil }
      .getMd5HashOfGoogleObject(uri) should be(Some(expectedHash))
  }

  it should "not fail when 'gsutil hash' doesn't output an md5 hash" in {
    val mockGsUtil = new GsUtil(None) {
      override def hash(path: String): String =
        s"""Hashes [hex] for pipeline/C1963/CHMI_CHMI3_Nex1/v1/id.txt:
           |        Hash (crc32c):          392eca9a
         """.stripMargin
    }

    new IoUtil { override val gsUtil: GsUtil = mockGsUtil }
      .getMd5HashOfGoogleObject(uri) should be(None)
  }

}
