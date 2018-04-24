package org.broadinstitute.clio.client.util

import java.io.{IOException, PrintWriter, StringWriter}
import java.net.URI

import akka.stream.scaladsl.Sink
import better.files.File
import org.broadinstitute.clio.client.BaseClientSpec
import org.scalatest.AsyncTestSuite

import scala.collection.immutable

class IoUtilSpec extends BaseClientSpec with AsyncTestSuite {
  behavior of "IoUtil"

  class TestIoUtil extends IoUtil {

    override def readGoogleObjectData(location: URI): String = { " " }

    override def writeGoogleObjectData(data: String, location: URI): Unit = {}

    override def copyGoogleObject(from: URI, to: URI): Unit = {}

    override def deleteGoogleObject(path: URI): Unit = {}

    override def googleObjectExists(path: URI): Boolean = { false }
  }

  it should "read a metadata file from file location" in {
    val contents = "I'm a file!"

    File.temporaryFile() { f =>
      new TestIoUtil().readMetadata(f.write(contents).uri) should be(contents)
    }
  }

  it should "build streams for deleting multiple cloud objects" in {
    val uris = immutable.Iterable(
      URI.create("gs://path/to/the.object"),
      URI.create("gs://path/to/the/other.object")
    )

    var deleted: List[URI] = List()

    val gsutil = new TestIoUtil() {
      override def deleteGoogleObject(path: URI): Unit = {
        deleted = deleted :+ path
      }
    }

    for {
      result <- gsutil.deleteCloudObjects(uris).runWith(Sink.head)
    } yield {
      result should be(())
      deleted should be(uris)
    }
  }

  it should "not fail when building a stream for zero deletes" in {
    val stream = new TestIoUtil().deleteCloudObjects(immutable.Iterable.empty)
    stream.runWith(Sink.head).map(_ should be(()))
  }

  it should "include all failures in the exception message when parallel deletes fail" in {
    val urisToFail =
      Set("gs://path/to/the.object", "gs://path/to/the/other.object").map(URI.create)
    val uriToSucceed = URI.create("gs://some/other/object")

    var deleted: List[URI] = List()

    val gsutil = new TestIoUtil() {
      override def deleteGoogleObject(path: URI): Unit = {
        if (urisToFail.contains(path)) {
          throw new IOException(path.toString)
        }
        deleted = deleted :+ path
      }
    }

    val stream = gsutil.deleteCloudObjects(urisToFail + uriToSucceed)

    for {
      _ <- recoverToExceptionIf[IOException](stream.runWith(Sink.ignore)).map { ex =>
        urisToFail.foreach { uri =>
          val sw = new StringWriter
          ex.printStackTrace(new PrintWriter(sw))
          sw.toString should include(uri.toString)
        }
        succeed
      }
    } yield {
      deleted should be(Seq(uriToSucceed))
    }
  }
}
