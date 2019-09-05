package org.broadinstitute.clio.client.dispatch

import java.io.{PrintWriter, StringWriter}
import java.net.URI

import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeleteArrays
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.{ArraysKey, ArraysMetadata}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}
import org.scalamock.scalatest.AsyncMockFactory

import scala.collection.immutable
import scala.concurrent.ExecutionContext

class DeleteExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "DeleteExecutor"

  // Calling it 'key' clashes with a ScalaTest member...
  private val theKey = ArraysKey(
    chipwellBarcode = Symbol("abcdefg"),
    version = 1,
    location = Location.GCP
  )
  private val metadata = ArraysMetadata(
    vcfPath = Some(URI.create("gs://the-vcf")),
    vcfIndexPath = Some(URI.create("gs://the-vcf-tbi")),
    gtcPath = Some(URI.create("gs://the-gtc")),
    paramsPath = Some(URI.create("gs://the-params")),
    documentStatus = Some(DocumentStatus.Normal)
  )
  private val theNote = "Testing testing"
  private val id = UpsertId.nextId()

  private val serverErr = ClioWebClient
    .FailedResponse(StatusCodes.InternalServerError, "I BROKE")

  type Aux = ClioWebClient.UpsertAux[ArraysKey, ArraysMetadata]

  it should "delete cloud records" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: ArraysKey, _: Boolean))
      .expects(ArraysIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = metadata.pathsToDelete.to[immutable.Iterable]
    paths.foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
      (ioUtil.googleObjectExists _).expects(uri).returning(true)
    }

    (ioUtil
      .deleteCloudGenerations(_: immutable.Iterable[URI])(_: ExecutionContext))
      .expects(paths, executionContext)
      .returning(Source.single(()))

    (webClient
      .upsert(_: Aux)(_: ArraysKey, _: ArraysMetadata, _: Boolean))
      .expects(
        ArraysIndex,
        theKey,
        metadata.copy(
          documentStatus = Some(DocumentStatus.Deleted),
          notes = Some(theNote)
        ),
        true
      )
      .returning(Source.single(id.asJson))

    val executor = new DeleteExecutor(DeleteArrays(theKey, theNote))
    executor.execute(webClient, ioUtil).runWith(Sink.head).map { json =>
      json.as[UpsertId] should be(Right(id))
    }
  }

  it should "not delete on-prem records" in {
    val executor =
      new DeleteExecutor(DeleteArrays(theKey.copy(location = Location.OnPrem), theNote))
    recoverToSucceededIf[UnsupportedOperationException] {
      executor.execute(stub[ClioWebClient], stub[IoUtil]).runWith(Sink.ignore)
    }
  }

  it should "fail if querying the server for existing metadata fails" in {
    val webClient = mock[ClioWebClient]

    (webClient
      .getMetadataForKey(_: Aux)(_: ArraysKey, _: Boolean))
      .expects(ArraysIndex, theKey, false)
      .returning(Source.failed(serverErr))

    val executor = new DeleteExecutor(DeleteArrays(theKey, theNote))
    recoverToSucceededIf[ClioWebClient.FailedResponse] {
      executor.execute(webClient, stub[IoUtil]).runWith(Sink.ignore)
    }
  }

  it should "fail if trying to delete an unregistered document" in {
    val webClient = mock[ClioWebClient]

    (webClient
      .getMetadataForKey(_: Aux)(_: ArraysKey, _: Boolean))
      .expects(ArraysIndex, theKey, false)
      .returning(Source.empty)

    val executor = new DeleteExecutor(DeleteArrays(theKey, theNote))
    recoverToSucceededIf[IllegalStateException] {
      executor.execute(webClient, stub[IoUtil]).runWith(Sink.ignore)
    }
  }

  it should "fail if on-prem paths are registered to a cloud document" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    (webClient
      .getMetadataForKey(_: Aux)(_: ArraysKey, _: Boolean))
      .expects(ArraysIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = metadata.pathsToDelete.to[immutable.Iterable]
    paths.take(2).foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
    }
    paths.drop(2).foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(false)
    }

    val executor = new DeleteExecutor(DeleteArrays(theKey, theNote))
    recoverToExceptionIf[IllegalStateException] {
      executor.execute(webClient, ioUtil).runWith(Sink.ignore)
    }.map { ex =>
      paths.drop(2).foreach { uri =>
        ex.getMessage should include(uri.toString)
      }
      succeed
    }
  }

  it should "fail if a cloud file doesn't exist and force=false" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: ArraysKey, _: Boolean))
      .expects(ArraysIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = metadata.pathsToDelete.to[immutable.Iterable]
    paths.foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
      (ioUtil.googleObjectExists _).expects(uri).returning(false)
    }

    val executor = new DeleteExecutor(DeleteArrays(theKey, theNote))
    recoverToExceptionIf[IllegalStateException] {
      executor.execute(webClient, ioUtil).runWith(Sink.ignore)
    }.map { ex =>
      val sw = new StringWriter
      ex.printStackTrace(new PrintWriter(sw))
      val errorText = sw.toString
      paths.foreach { uri =>
        errorText should include(uri.toString)
      }
      succeed
    }
  }

  it should "not fail if a cloud file doesn't exist but force=true" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: ArraysKey, _: Boolean))
      .expects(ArraysIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = metadata.pathsToDelete.to[immutable.Iterable]
    paths.foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
      (ioUtil.googleObjectExists _).expects(uri).returning(false)
    }

    (ioUtil
      .deleteCloudGenerations(_: immutable.Iterable[URI])(_: ExecutionContext))
      .expects(immutable.Iterable.empty[URI], executionContext)
      .returning(Source.single(()))

    (webClient
      .upsert(_: Aux)(_: ArraysKey, _: ArraysMetadata, _: Boolean))
      .expects(
        ArraysIndex,
        theKey,
        metadata.copy(
          documentStatus = Some(DocumentStatus.Deleted),
          notes = Some(theNote)
        ),
        true
      )
      .returning(Source.single(id.asJson))

    val executor = new DeleteExecutor(DeleteArrays(theKey, theNote, force = true))
    executor.execute(webClient, ioUtil).runWith(Sink.head).map { json =>
      json.as[UpsertId] should be(Right(id))
    }
  }

  it should "fail if the upsert to the server fails" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    // Type annotations needed for scalamockery.
    (webClient
      .getMetadataForKey(_: Aux)(_: ArraysKey, _: Boolean))
      .expects(ArraysIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = metadata.pathsToDelete.to[immutable.Iterable]
    paths.foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(true)
      (ioUtil.googleObjectExists _).expects(uri).returning(true)
    }

    (ioUtil
      .deleteCloudGenerations(_: immutable.Iterable[URI])(_: ExecutionContext))
      .expects(paths, executionContext)
      .returning(Source.single(()))

    (webClient
      .upsert(_: Aux)(_: ArraysKey, _: ArraysMetadata, _: Boolean))
      .expects(
        ArraysIndex,
        theKey,
        metadata.copy(
          documentStatus = Some(DocumentStatus.Deleted),
          notes = Some(theNote)
        ),
        true
      )
      .returning(Source.failed(serverErr))

    val executor = new DeleteExecutor(DeleteArrays(theKey, theNote))
    recoverToSucceededIf[RuntimeException] {
      executor.execute(webClient, ioUtil).runWith(Sink.ignore)
    }
  }
}
