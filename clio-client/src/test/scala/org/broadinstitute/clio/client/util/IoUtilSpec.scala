package org.broadinstitute.clio.client.util

import java.io.{IOException, PrintWriter, StringWriter}
import java.net.URI

import akka.stream.scaladsl.Sink
import better.files.File
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.transfer.model.{CramIndex, UbamIndex}
import org.broadinstitute.clio.transfer.model.ubam.UbamMetadata
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncTestSuite

import scala.collection.JavaConverters._
import scala.collection.immutable

class IoUtilSpec extends BaseClientSpec with AsyncTestSuite with AsyncMockFactory {
  behavior of "IoUtil"

  private def uriToBlobInfo(uri: URI) = {
    BlobInfo.newBuilder(IoUtil.toBlobId(uri)).build()
  }

  private def createStorage = {
    LocalStorageHelper.getOptions.getService
  }

  it should "read a file from file location" in {
    val contents = "I'm a file!"

    File.temporaryFile() { f =>
      new IoUtil(createStorage)
        .readFile(f.write(contents).uri) should be(contents)
    }
  }

  it should "identify google directories" in {
    val ioUtil = new IoUtil(createStorage)
    ioUtil.isGoogleDirectory(URI.create("gs://bucket/directory/")) should be(true)
    ioUtil.isGoogleDirectory(URI.create("gs://bucket/file.txt")) should be(false)
    ioUtil.isGoogleDirectory(URI.create("foo")) should be(false)
  }

  it should "build streams for deleting multiple cloud objects" in {
    val uris = immutable.Iterable(
      URI.create("gs://bucket/to/the.object"),
      URI.create("gs://bucket/to/the/other.object")
    )

    val storage = createStorage
    uris.foreach(uri => storage.create(uriToBlobInfo(uri)))

    val ioUtil = new IoUtil(storage)

    ioUtil.deleteCloudObjects(uris).runWith(Sink.head).map { result =>
      result should be(())
      storage.list("bucket").getValues.asScala should be(empty)
    }
  }

  it should "not fail when building a stream for zero deletes" in {
    val stream = new IoUtil(createStorage)
      .deleteCloudObjects(immutable.Iterable.empty)
    stream.runWith(Sink.head).map(_ should be(()))
  }

  it should "include all failures in the exception message when parallel deletes fail" in {
    val urisToFail =
      Set("gs://path/to/the.object", "gs://path/to/the/other.object").map(URI.create)
    val uriToSucceed = URI.create("gs://some/other/object")

    val storage = createStorage
    storage.create(uriToBlobInfo(uriToSucceed))

    val stream = new IoUtil(storage).deleteCloudObjects(urisToFail + uriToSucceed)

    recoverToExceptionIf[IOException](stream.runWith(Sink.ignore)).map { ex =>
      val sw = new StringWriter
      ex.printStackTrace(new PrintWriter(sw))
      val errorText = sw.toString
      urisToFail.foreach { uri =>
        errorText should include(uri.toString)
      }
      storage.list("bucket").getValues.asScala should be(empty)
    }
  }

  it should "read google object data" in {
    val location = URI.create("gs://bucket/path/data")
    val contents = "my data"
    val storage = createStorage
    storage.create(uriToBlobInfo(location), contents.getBytes)

    new IoUtil(storage).readFile(location) should be(contents)
  }

  it should "write google object data" in {
    val location = URI.create("gs://bucket/path/data")
    val contents = "my data"
    val storage = createStorage

    new IoUtil(storage).writeGoogleObjectData(contents, location)

    storage.readAllBytes(IoUtil.toBlobId(location)) should be(contents.getBytes)
  }

  it should "write google object data when the file already exists" in {
    val location = URI.create("gs://bucket/path/data")
    val contents = "my data"
    val storage = createStorage
    storage.create(uriToBlobInfo(location), "original data".getBytes)

    new IoUtil(storage).writeGoogleObjectData(contents, location)

    storage.readAllBytes(IoUtil.toBlobId(location)) should be(contents.getBytes)
  }

  it should "detect if a google object exists or not" in {
    val location = URI.create("gs://bucket/path/data")
    val storage = createStorage
    val ioutil = new IoUtil(storage)

    ioutil.googleObjectExists(location) should be(false)
    storage.create(uriToBlobInfo(location), "data".getBytes)
    ioutil.googleObjectExists(location) should be(true)
  }

  it should "copy a google object" in {
    val source = URI.create("gs://bucket/path/data")
    val destination = URI.create("gs://bucket/path/newdata")
    val contents = "my data"
    val storage = createStorage
    storage.create(uriToBlobInfo(source), contents.getBytes)

    new IoUtil(storage).copyGoogleObject(source, destination)

    storage.readAllBytes(IoUtil.toBlobId(destination)) should be(contents.getBytes)
  }

  it should "list all children of objects" in {
    val source = URI.create("gs://bucket/path/data")
    val source2 = URI.create("gs://bucket/path/data2")
    val expected = Seq(source, source2)
    val contents = "my data"
    val storage = createStorage
    storage.create(uriToBlobInfo(source), contents.getBytes)
    storage.create(uriToBlobInfo(source2), contents.getBytes)

    val ioUtil = new IoUtil(storage)

    ioUtil.listGoogleObjects(URI.create("gs://bucket/path/")) should contain theSameElementsAs expected

  }

  it should "parse a metadata json" in {
    val metadata = UbamMetadata(ubamSize = Some(10l), ubamMd5 = Some(Symbol("md5")))
    val metadataFile =
      File.newTemporaryFile().deleteOnExit().write(metadata.asJson.pretty(implicitly))
    new IoUtil(createStorage)
      .readMetadata(UbamIndex)(metadataFile.uri)
      .map { readMetadata =>
        readMetadata should be(metadata)
      }
      .runWith(Sink.head)
  }

  it should "fail to decode incorrect metadata types" in {
    val metadata = UbamMetadata(ubamSize = Some(10l), ubamMd5 = Some(Symbol("md5")))
    val metadataFile =
      File.newTemporaryFile().deleteOnExit().write(metadata.asJson.pretty(implicitly))
    recoverToSucceededIf[IllegalArgumentException] {
      new IoUtil(createStorage)
        .readMetadata(CramIndex)(metadataFile.uri)
        .runWith(Sink.head)
    }
  }

  it should "fail to parse invalid json" in {
    val metadataFile = File.newTemporaryFile().deleteOnExit().write("{not valid JSON'")

    recoverToSucceededIf[IllegalArgumentException] {
      new IoUtil(createStorage)
        .readMetadata(CramIndex)(metadataFile.uri)
        .runWith(Sink.ignore)
    }
  }
}
