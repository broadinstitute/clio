package org.broadinstitute.clio.client.util

import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.scaladsl.Source
import better.files._
import cats.syntax.either._
import com.google.api.gax.retrying.RetrySettings
import com.google.cloud.http.HttpTransportOptions
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, StorageOptions}
import com.typesafe.scalalogging.StrictLogging
import io.circe.parser.parse
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.auth.ClioCredentials
import org.threeten.bp.Duration

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Clio-client component handling all file IO operations.
  */
class IoUtil(storage: Storage) extends StrictLogging {

  import IoUtil._

  def isGoogleObject(location: URI): Boolean =
    Option(location.getScheme).contains(GoogleCloudStorageScheme)

  def isGoogleDirectory(location: URI): Boolean =
    isGoogleObject(location) && location.getPath.endsWith("/")

  def readFile(location: URI): String = {
    if (isGoogleObject(location)) {
      readGoogleObjectData(location)
    } else {
      readFileData(location)
    }
  }

  def readFileData(location: URI): String =
    File(location.getPath).contentAsString

  /**
    * Build a stream which, when pulled, will read JSON from a URI and decode it
    * into the metadata type associated with `index`.
    */
  def readMetadata[CI <: ClioIndex](index: ClioIndex)(
    location: URI
  ): Source[index.MetadataType, NotUsed] = {
    import index.implicits._
    Source
      .single(location)
      .map(readFile)
      .map {
        parse(_).valueOr { err =>
          throw new IllegalArgumentException(
            s"Could not parse contents of $location as JSON.",
            err
          )
        }
      }
      .map {
        _.as[index.MetadataType].valueOr { err =>
          throw new IllegalArgumentException(s"Invalid metadata given at $location.", err)
        }
      }
  }

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

  /**
    * Build a stream which, when pulled, will delete all generations
    * of the given cloud paths in parallel.
    */
  def deleteCloudGenerations(
    paths: immutable.Iterable[URI]
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    val generations = paths.flatMap(path => listGoogleGenerations(path))
    Source(generations)
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
            new IOException("Failed to delete cloud generations", head)
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
    logger.info(s"Writing to '$location'...")
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
    logger.info(s"Copying '$from' to '$to'...")
    requireBlob(from).copyTo(toBlobId(to)).getResult
    ()
  }

  def deleteGoogleObject(path: URI): Unit = {
    logger.info(s"Deleting '$path'...")
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
    val blobId = IoUtil.toBlobId(path)
    storage
      .list(
        blobId.getBucket,
        BlobListOption.prefix(blobId.getName)
      )
      .iterateAll()
      .asScala
      .map(b => URI.create(s"$GoogleCloudPathPrefix${b.getBucket}/${b.getName}"))
      .toList
  }

  def listGoogleGenerations(path: URI): Seq[URI] = {
    val blobId = IoUtil.toBlobId(path)
    storage
      .list(
        blobId.getBucket,
        BlobListOption.prefix(blobId.getName),
        BlobListOption.versions(true)
      )
      .iterateAll()
      .asScala
      .map(
        b =>
          URI.create(
            s"$GoogleCloudPathPrefix${b.getBucket}/${b.getName}#${b.getGeneration}"
        )
      )
      .toList
  }
}

object IoUtil {

  val GoogleCloudGeneration = "#"
  val GoogleCloudPathSeparator = "/"
  val GoogleCloudStorageScheme = "gs"
  val GoogleCloudPathPrefix = GoogleCloudStorageScheme + "://"

  def apply(credentials: ClioCredentials): IoUtil = {
    new IoUtil(
      StorageOptions
        .newBuilder()
        .setCredentials(credentials.storage(readOnly = false))
        // These settings are extremely generous timeouts and retries to make google cloud operations less error prone
        // Pulled from Picard and GATK
        .setTransportOptions(
          HttpTransportOptions.newBuilder
            .setConnectTimeout(120000)
            .setReadTimeout(120000)
            .build
        )
        .setRetrySettings(
          RetrySettings.newBuilder
            .setMaxAttempts(15)
            .setMaxRetryDelay(Duration.ofMillis(256000))
            .setTotalTimeout(Duration.ofMillis(4000000))
            .setInitialRetryDelay(Duration.ofMillis(1000))
            .setRetryDelayMultiplier(2.0)
            .setInitialRpcTimeout(Duration.ofMillis(180000))
            .setRpcTimeoutMultiplier(1.0)
            .setMaxRpcTimeout(Duration.ofMillis(180000))
            .build
        )
        .build()
        .getService
    )
  }

  def toBlobId(path: URI): BlobId = {
    val noPrefix = path.toString.substring(GoogleCloudPathPrefix.length)
    val firstSeparator = noPrefix.indexOf(GoogleCloudPathSeparator)
    val bucket = noPrefix.substring(0, firstSeparator)
    val generationIndex = noPrefix.lastIndexOf(GoogleCloudGeneration)
    if (-1 == generationIndex) {
      val blob = noPrefix.substring(firstSeparator + 1)
      BlobId.of(bucket, blob)
    } else {
      val blob = noPrefix.substring(firstSeparator + 1, generationIndex)
      val generationString = noPrefix.substring(generationIndex + 1)
      val generation = generationString.toLong
      BlobId.of(bucket, blob, generation)
    }
  }
}
