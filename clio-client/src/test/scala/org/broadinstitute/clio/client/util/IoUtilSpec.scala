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
    val mockGsUtil = new GsUtil {
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
    val mockGsUtil = new GsUtil {
      override def hash(path: String): String =
        s"""Hashes [hex] for pipeline/C1963/CHMI_CHMI3_Nex1/v1/id.txt:
           |        Hash (crc32c):          392eca9a
         """.stripMargin
    }

    new IoUtil { override val gsUtil: GsUtil = mockGsUtil }
      .getMd5HashOfGoogleObject(uri) should be(None)
  }

  it should "parse object size and hash out of 'gsutil stat' output" in {
    val expectedSize = 123412L
    val expectedMd5Hash = Symbol("d41d8cd98f00b204e9800998ecf8427e")

    val mockGsUtil = new GsUtil {
      override def stat(path: String): String =
        s"""    Creation time:          Wed, 24 Jan 2018 23:00:14 GMT
           |    Update time:            Wed, 24 Jan 2018 23:00:14 GMT
           |    Storage class:          STANDARD
           |    Content-Length:         $expectedSize
           |    Content-Type:           application/octet-stream
           |    Hash (crc32c):          AAAAAA==
           |    Hash (md5):             1B2M2Y8AsgTpgAmY7PhCfg==
           |    ETag:                   CKCYzILa8dgCEAE=
           |    Generation:             1516834814888992
           |    Metageneration:         1
         """.stripMargin
    }

    new IoUtil { override val gsUtil: GsUtil = mockGsUtil }
      .getGoogleObjectInfo(uri) should be(Some(expectedSize -> Some(expectedMd5Hash)))
  }

  it should "not fail when 'gsutil stat' doesn't output an md5 hash" in {
    val expectedSize = 123412L

    val mockGsUtil = new GsUtil {
      override def stat(path: String): String =
        s"""    Creation time:          Wed, 24 Jan 2018 23:00:14 GMT
           |    Update time:            Wed, 24 Jan 2018 23:00:14 GMT
           |    Storage class:          STANDARD
           |    Content-Length:         $expectedSize
           |    Content-Type:           application/octet-stream
           |    Hash (crc32c):          AAAAAA==
           |    ETag:                   CKCYzILa8dgCEAE=
           |    Generation:             1516834814888992
           |    Metageneration:         1
         """.stripMargin
    }

    new IoUtil { override val gsUtil: GsUtil = mockGsUtil }
      .getGoogleObjectInfo(uri) should be(Some(expectedSize -> None))
  }

  it should "return None for object info when 'gsutil stat' fails" in {

    val mockGsUtil = new GsUtil {
      override def stat(path: String): String = sys.error("Nonzero retcode")
    }

    new IoUtil { override val gsUtil: GsUtil = mockGsUtil }
      .getGoogleObjectInfo(uri) should be(None)
  }

}
