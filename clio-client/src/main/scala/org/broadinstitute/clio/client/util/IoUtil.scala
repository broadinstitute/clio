package org.broadinstitute.clio.client.util

import java.io.IOException
import java.math.BigInteger
import java.net.URI
import java.util.Base64

import akka.NotUsed
import akka.stream.scaladsl.Source
import better.files.File
import cats.syntax.either._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.{Process, ProcessBuilder}

/**
  * Clio-client component handling all file IO operations.
  */
trait IoUtil {

  import IoUtil._

  protected def gsUtil: GsUtil

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
      .flatMapConcat { errs =>
        if (errs.isEmpty) {
          Source.single(())
        } else {
          Source.failed(
            new IOException(
              s"""Failed to delete cloud objects:
                 |${errs.map(_.getMessage).mkString("\n")}""".stripMargin
            )
          )
        }
      }
  }

  /*
   * FIXME for all below:
   *
   * Add a "credentials" argument (explicit or implicit) and
   * use it to authenticate to GCS so we can convert the URIs
   * into Paths and use the NIO API.
   *
   * We can't just parse as Paths from the start because the
   * google-cloud-nio adapter will eagerly check storage authentication
   * for each path, and fail.
   */

  def readGoogleObjectData(location: URI): String = {
    gsUtil.cat(location.toString)
  }

  def copyGoogleObject(from: URI, to: URI): Unit = {
    if (gsUtil.cp(from.toString, to.toString) != 0) {
      throw new IOException(s"Failed to copy $from to $to in the cloud.")
    }
  }

  def deleteGoogleObject(path: URI): Unit = {
    if (gsUtil.rm(path.toString) != 0) {
      throw new IOException(s"Failed to delete $path in the cloud.")
    }
  }

  def googleObjectExists(path: URI): Boolean = {
    gsUtil.exists(path.toString) == 0
  }

  def getMd5HashOfGoogleObject(path: URI): Option[Symbol] = {
    /*
     * Files uploaded through parallel composite uploads won't have an md5 hash.
     * See https://cloud.google.com/storage/docs/gsutil/commands/cp#parallel-composite-uploads
     */
    val rawHash = gsUtil.hash(path.toString)
    md5HexPattern.findFirstMatchIn(rawHash).map(m => Symbol(m.group(1)))
  }

  def getSizeOfGoogleObject(path: URI): Long = {
    gsUtil
      .du(path.toString)
      .head
      .split("\\s+")
      .head
      .toLong
  }

  def listGoogleObjects(path: URI): Seq[String] = {
    gsUtil.ls(path.toString)
  }

  /**
    * Check if a path points to an object in GCS.
    *
    * If so, return the object's size and md5 hash (if present).
    */
  def getGoogleObjectInfo(path: URI): (Long, Option[Symbol]) = {
    val rawStat = gsUtil.stat(path.toString)
    val objectSize = sizePattern
      .findFirstMatchIn(rawStat)
      .fold(
        throw new IllegalStateException(s"No size reported for google object at $path")
      )(_.group(1))
    val base64Hash = md5Base64Pattern.findFirstMatchIn(rawStat).map(_.group(1))
    val decoded = base64Hash.map(Base64.getDecoder.decode(_))
    val encoded = decoded.map(bytes => f"${new BigInteger(1, bytes)}%x")

    (objectSize.toLong, encoded.map(Symbol.apply))
  }
}

object IoUtil extends IoUtil {

  private val sizePattern = "Content-Length:\\s+([0-9]+)".r
  private val md5Base64Pattern = "Hash \\(md5\\):\\s+(.+)".r
  private val md5HexPattern = "Hash \\(md5\\):\\s+([0-9a-f]+)".r

  val GoogleCloudStorageScheme = "gs"

  override val gsUtil: GsUtil = new GsUtil

  /**
    * Wrapper around gsutil managing state directory creation
    * and basic result parsing.
    *
    * TODO: We should consider moving this to google-cloud-java
    * api usage instead of gsutil. We pay significant overhead
    * on the startup time of every gsutil call.
    */
  class GsUtil {

    def ls(path: String): Seq[String] = {
      runGsUtilAndGetStdout(Seq("ls", path)).split("\n")
    }

    def du(path: String): Seq[String] = {
      runGsUtilAndGetStdout(Seq("du", path)).split("\n")
    }

    def cp(from: String, to: String): Int = {
      runGsUtilAndGetExitCode(Seq("cp", from, to))
    }

    def mv(from: String, to: String): Int = {
      runGsUtilAndGetExitCode(Seq("mv", from, to))
    }

    def rm(path: String): Int = {
      runGsUtilAndGetExitCode(Seq("rm", "-a", path))
    }

    def cat(objectLocation: String): String = {
      runGsUtilAndGetStdout(Seq("cat", objectLocation))
    }

    def exists(path: String): Int = {
      runGsUtilAndGetExitCode(Seq("-q", "stat", path))
    }

    def hash(path: String): String = {
      runGsUtilAndGetStdout(Seq("hash", "-m", "-h", path))
    }

    def stat(path: String): String = {
      runGsUtilAndGetStdout(Seq("stat", path))
    }

    private val runGsUtilAndGetStdout = runGsUtil(_.!!)(_)

    private val runGsUtilAndGetExitCode = runGsUtil(_.!)(_)

    private def runGsUtil[Out](
      runner: ProcessBuilder => Out
    )(gsUtilArgs: Seq[String]): Out = {
      File
        .temporaryDirectory("gsutil-state")
        .map { tmp =>
          runner(Process(Seq("gsutil", "-o", s"GSUtil:state_dir=$tmp") ++ gsUtilArgs))
        }
        .get()
    }
  }
}
