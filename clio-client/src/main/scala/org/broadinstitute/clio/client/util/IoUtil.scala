package org.broadinstitute.clio.client.util

import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.scaladsl.Source
import better.files.File
import cats.syntax.either._
import com.google.auth.Credentials
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, StorageOptions}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Clio-client component handling all file IO operations.
  */
trait IoUtil {

  import IoUtil._

  def isGoogleObject(location: URI): Boolean =
    Option(location.getScheme).contains(GoogleCloudStorageScheme)

  def isGoogleDirectory(location: URI): Boolean =
    isGoogleObject(location) && location.getPath.endsWith("/")

  def readMetadata(location: URI): String = {
    if (isGoogleObject(location)) {
      readGoogleObjectData(location)
    } else {
      readFileData(location)
    }
  }

  def readFileData(location: URI): String =
    File(location.getPath).contentAsString

  /**
    * Build a stream which, when pulled, will delete all of the
    * given cloud objects in parallel.
    */
  def deleteCloudObjects(
    paths: immutable.Iterable[URI]
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    Source(paths)
      .mapAsyncUnordered(paths.size + 1) { path =>
        Future(Either.catchNonFatal(deleteGoogleObject(path)))
      }
      .fold(Seq.empty[Throwable]) { (acc, attempt) =>
        attempt.fold(acc :+ _, _ => acc)
      }
      .flatMapConcat {
        case Seq() => Source.single(())
        case head +: tail =>
          val exception =
            new IOException("Failed to delete cloud objects", head)
          tail.foreach(exception.addSuppressed)
          Source.failed(exception)
      }
  }

  def readGoogleObjectData(location: URI): String

  def writeGoogleObjectData(data: String, location: URI): Unit

  def copyGoogleObject(from: URI, to: URI): Unit

  def deleteGoogleObject(path: URI): Unit

  def googleObjectExists(path: URI): Boolean
}

object IoUtil {
  val GoogleCloudStorageScheme = "gs"
}

class GsUtil(credentials: Credentials) extends IoUtil {

  private val storage = {
    val storageOptions = StorageOptions
      .newBuilder()
      .setCredentials(credentials)
      .build()

    storageOptions.getService
  }

  private val GoogleCloudPathPrefix = IoUtil.GoogleCloudStorageScheme + "//"
  private val GoogleCloudPathSeparator = "/"

  private def toBlobId(path: URI) = {
    val noPrefix = path.toString.substring(GoogleCloudPathPrefix.length + 1)
    val firstSeparator = noPrefix.indexOf(GoogleCloudPathSeparator)
    // Get the bucket and the object name (aka path) from the gcs path.
    BlobId.of(
      noPrefix.substring(0, firstSeparator),
      noPrefix.substring(firstSeparator + 1)
    )
  }

  private def getBlob(path: URI) = {
    storage.get(toBlobId(path))
  }

  override def readGoogleObjectData(location: URI): String = {
    new String(getBlob(location).getContent())
  }

  override def writeGoogleObjectData(data: String, location: URI): Unit = {
    getBlob(location) match {
      case blob: Blob =>
        val channel = blob.writer()
        try {
          channel.write(ByteBuffer.wrap(data.getBytes()))
        } finally {
          channel.close()
        }
      case _ =>
        val info = BlobInfo.newBuilder(toBlobId(location)).build()
        storage.create(info, data.getBytes())
    }
    ()
  }

  override def copyGoogleObject(from: URI, to: URI): Unit = {
    getBlob(from).copyTo(toBlobId(to))
    ()
  }

  override def deleteGoogleObject(path: URI): Unit = {
    if (!getBlob(path).delete()) {
      throw new RuntimeException(s"Cannot delete '$path' because it does not exist.")
    }
  }

  override def googleObjectExists(path: URI): Boolean = {
    getBlob(path) match {
      case blob: Blob => blob.exists()
      case _          => false
    }
  }
}
