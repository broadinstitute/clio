package org.broadinstitute.clio.client.util

import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.scaladsl.Source
import better.files._
import cats.syntax.either._
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, StorageOptions}
import org.broadinstitute.clio.util.auth.ClioCredentials

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Clio-client component handling all file IO operations.
  */
class IoUtil(storage: Storage) {

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

  def getBlob(path: URI): Option[Blob] = {
    Option(storage.get(toBlobId(path)))
  }

  def requireBlob(path: URI): Blob = {
    getBlob(path).getOrElse(
      throw new IllegalArgumentException(s"Invalid google path $path")
    )
  }

  def readGoogleObjectData(location: URI): String = {
    new String(requireBlob(location).getContent())
  }

  def writeGoogleObjectData(data: String, location: URI): Unit = {
    getBlob(location) match {
      case Some(blob) =>
        blob.writer().autoClosed { channel =>
          channel.write(ByteBuffer.wrap(data.getBytes()))
          ()
        }
      case None =>
        val info = BlobInfo.newBuilder(toBlobId(location)).build()
        storage.create(info, data.getBytes())
    }
    ()
  }

  def copyGoogleObject(from: URI, to: URI): Unit = {
    requireBlob(from).copyTo(toBlobId(to)).getResult
    ()
  }

  def deleteGoogleObject(path: URI): Unit = {
    if (!storage.delete(toBlobId(path))) {
      throw new IllegalArgumentException(
        s"Cannot delete '$path' because it does not exist."
      )
    }
  }

  def googleObjectExists(path: URI): Boolean = {
    getBlob(path).exists(_.exists())
  }

  def listGoogleObjects(path: URI): Seq[URI] = {
    val blobId = requireBlob(path)
    storage
      .list(
        blobId.getBucket,
        BlobListOption.currentDirectory(),
        BlobListOption.prefix(blobId.getName)
      )
      .iterateAll()
      .asScala
      .map(b => URI.create(s"$GoogleCloudPathPrefix${b.getBucket}/${b.getName}"))
      .toList
  }
}

object IoUtil {

  private val GoogleCloudStorageScheme = "gs"
  private val GoogleCloudPathPrefix = GoogleCloudStorageScheme + "://"
  private val GoogleCloudPathSeparator = "/"

  def apply(credentials: ClioCredentials): IoUtil = {
    new IoUtil(
      StorageOptions
        .newBuilder()
        .setCredentials(credentials.storage(readOnly = false))
        .build()
        .getService
    )
  }

  def toBlobId(path: URI): BlobId = {
    val noPrefix = path.toString.substring(GoogleCloudPathPrefix.length)
    val firstSeparator = noPrefix.indexOf(GoogleCloudPathSeparator)
    // Get the bucket and the object name (aka path) from the gcs path.
    BlobId.of(
      noPrefix.substring(0, firstSeparator),
      noPrefix.substring(firstSeparator + 1)
    )
  }
}
