package org.broadinstitute.clio.client.util

import java.io.IOException
import java.net.URI

import akka.stream.scaladsl.Sink
import better.files.File
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.util.IoUtil.GsUtil
import org.scalamock.scalatest.AsyncMockFactory

import scala.collection.immutable

class IoUtilSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "IoUtil"

  val uri = URI.create(
    "gs://broad-gotc-dev-storage/pipeline/C1963/CHMI_CHMI3_Nex1/v1/id.txt"
  )

  it should "read a metadata file from file location" in {
    val contents = "I'm a file!"

    File.temporaryFile() { f =>
      IoUtil.readMetadata(f.write(contents).uri) should be(contents)
    }
  }

  it should "parse the md5 hash out of 'gsutil hash' output" in {
    val gsutil = stub[GsUtil]
    val expectedHash = Symbol("998e42da13d7bd619f798baf8ea08a13")

    (gsutil.hash _).when(uri.toString).returns {
      s"""Hashes [hex] for pipeline/C1963/CHMI_CHMI3_Nex1/v1/id.txt:
         |        Hash (crc32c):          392eca9a
         |        Hash (md5):             ${expectedHash.name}
       """.stripMargin
    }

    new IoUtil { override val gsUtil: GsUtil = gsutil }
      .getMd5HashOfGoogleObject(uri) should be(Some(expectedHash))
  }

  it should "not fail when 'gsutil hash' doesn't output an md5 hash" in {
    val gsutil = stub[GsUtil]

    (gsutil.hash _).when(uri.toString).returns {
      """Hashes [hex] for pipeline/C1963/CHMI_CHMI3_Nex1/v1/id.txt:
        |        Hash (crc32c):          392eca9a
      """.stripMargin
    }

    new IoUtil { override val gsUtil: GsUtil = gsutil }
      .getMd5HashOfGoogleObject(uri) should be(None)
  }

  it should "parse object size and hash out of 'gsutil stat' output" in {
    val gsutil = stub[GsUtil]
    val expectedSize = 123412L
    val expectedMd5Hash = Symbol("d41d8cd98f00b204e9800998ecf8427e")

    (gsutil.stat _).when(uri.toString).returns {
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

    new IoUtil { override val gsUtil: GsUtil = gsutil }
      .getGoogleObjectInfo(uri) should be(expectedSize -> Some(expectedMd5Hash))
  }

  it should "not fail when 'gsutil stat' doesn't output an md5 hash" in {
    val gsutil = stub[GsUtil]
    val expectedSize = 123412L

    (gsutil.stat _).when(uri.toString).returns {
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

    new IoUtil { override val gsUtil: GsUtil = gsutil }
      .getGoogleObjectInfo(uri) should be(expectedSize -> None)
  }

  it should "build streams for copying multiple cloud objects" in {

    val uris = immutable.Iterable(
      URI.create("gs://path/to/the.object") -> URI.create(
        "gs://new/path/to/the.object"
      ),
      URI.create("gs://path/to/the/other.object") -> URI.create(
        "gs://new/path/to/the/other.object"
      )
    )

    val gsutil = mock[GsUtil]

    uris.foreach {
      case (src, dest) => (gsutil.cp _).expects(src.toString, dest.toString).returning(0)
    }

    val ioUtil = new IoUtil {
      override protected def gsUtil: GsUtil = gsutil
    }

    // Expectations built into the mock will fail if we don't call the right copies.
    ioUtil.copyCloudObjects(uris).runWith(Sink.head).map(_ => succeed)
  }

  it should "not fail when building a stream for zero copies" in {
    val ioUtil = new IoUtil {
      override protected def gsUtil: GsUtil = stub[GsUtil]
    }
    val stream = ioUtil.copyCloudObjects(immutable.Iterable.empty)
    stream.runWith(Sink.head).map(_ should be(()))
  }

  it should "include all failures in the exception message when parallel copies fail" in {
    val urisToFail = Set("gs://path/to/the.object", "gs://path/to/the/other.object")
    val uriToSucceed = "gs://some/other/object"

    val gsutil = mock[GsUtil]
    urisToFail.foreach(uri => (gsutil.cp _).expects(uri, *).returning(1))
    (gsutil.cp _).expects(uriToSucceed, *).returning(0)

    val ioUtil = new IoUtil {
      override protected def gsUtil: GsUtil = gsutil
    }

    val stream = ioUtil.copyCloudObjects(
      (urisToFail + uriToSucceed).map(URI.create).map(_ -> URI.create("gs://blah"))
    )

    recoverToExceptionIf[IOException](stream.runWith(Sink.ignore)).map { ex =>
      urisToFail.foreach { uri =>
        ex.getMessage should include(uri)
      }
      succeed
    }
  }

  it should "build streams for deleting multiple cloud objects" in {
    val uris = immutable.Iterable(
      URI.create("gs://path/to/the.object"),
      URI.create("gs://path/to/the/other.object")
    )

    val gsutil = mock[GsUtil]
    uris.foreach(uri => (gsutil.rm _).expects(uri.toString).returning(0))

    val ioUtil = new IoUtil {
      override protected def gsUtil: GsUtil = gsutil
    }

    // Expectations built into the mock will fail if we don't call the right deletes.
    ioUtil.deleteCloudObjects(uris).runWith(Sink.head).map(_ should be(()))
  }

  it should "not fail when building a stream for zero deletes" in {
    val ioUtil = new IoUtil {
      override protected def gsUtil: GsUtil = stub[GsUtil]
    }
    val stream = ioUtil.deleteCloudObjects(immutable.Iterable.empty)
    stream.runWith(Sink.head).map(_ should be(()))
  }

  it should "include all failures in the exception message when parallel deletes fail" in {
    val urisToFail = Set("gs://path/to/the.object", "gs://path/to/the/other.object")
    val uriToSucceed = "gs://some/other/object"

    val gsutil = mock[GsUtil]
    urisToFail.foreach(uri => (gsutil.rm _).expects(uri).returning(1))
    (gsutil.rm _).expects(uriToSucceed).returning(0)

    val ioUtil = new IoUtil {
      override protected def gsUtil: GsUtil = gsutil
    }

    val stream = ioUtil.deleteCloudObjects((urisToFail + uriToSucceed).map(URI.create))

    recoverToExceptionIf[IOException](stream.runWith(Sink.ignore)).map { ex =>
      urisToFail.foreach { uri =>
        ex.getMessage should include(uri)
      }
      succeed
    }
  }
}
