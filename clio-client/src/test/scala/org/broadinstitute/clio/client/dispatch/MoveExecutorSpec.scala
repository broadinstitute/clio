package org.broadinstitute.clio.client.dispatch

import java.io.{IOException, PrintWriter, StringWriter}
import java.net.URI

import scala.concurrent.ExecutionContext
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveCram
import org.broadinstitute.clio.client.dispatch.MoveExecutor.{CopyOp, MoveOp, WriteOp}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{CramIndex, Metadata}
import org.broadinstitute.clio.transfer.model.wgscram.{
  CramExtensions,
  CramKey,
  CramMetadata
}
import org.broadinstitute.clio.util.model.{DataType, Location, UpsertId}
import org.scalamock.scalatest.AsyncMockFactory

import scala.collection.immutable

class MoveExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "MoveExecutor"

  private val theKey = CramKey(
    location = Location.GCP,
    project = "the-project",
    sampleAlias = "the-sample",
    version = 1,
    dataType = DataType.WGS
  )
  private val metadata = CramMetadata(
    cramPath = Some(URI.create(s"gs://bucket/the-cram${CramExtensions.CramExtension}")),
    craiPath = Some(URI.create(s"gs://bucket/the-cram${CramExtensions.CraiExtension}"))
  )
  private val destination = URI.create("gs://the-destination/")
  private val id = UpsertId.nextId()

  private val serverErr = ClioWebClient
    .FailedResponse(StatusCodes.InternalServerError, "I BROKE")

  type Aux = ClioWebClient.UpsertAux[CramKey, CramMetadata]

  it should "perform IO ops" in {
    val move = MoveOp(URI.create("gs://the-src1"), URI.create("gs://the-dest1"))
    val copy = CopyOp(URI.create("gs://the-src2"), URI.create("gs://the-dest2"))
    val write = WriteOp("The contents", URI.create("gs://the-dest3"))

    val ioUtil = mock[IoUtil]
    (ioUtil.copyGoogleObject _).expects(move.src, move.dest).returning(())
    (ioUtil.copyGoogleObject _).expects(copy.src, copy.dest).returning(())
    (ioUtil.writeGoogleObjectData _).expects(write.contents, write.dest).returning(())

    MoveExecutor
      .runPreUpsertOps(immutable.Seq(move, copy, write), ioUtil)
      .runWith(Sink.seq)
      .map(_ => succeed)
  }

  it should "fail if any IO ops fail" in {
    val move = MoveOp(URI.create("gs://the-src1"), URI.create("gs://the-dest1"))
    val copy = CopyOp(URI.create("gs://the-src2"), URI.create("gs://the-dest2"))
    val write = WriteOp("The contents", URI.create("gs://the-dest3"))

    val msg = "copy failed"

    val ioUtil = mock[IoUtil]
    (ioUtil.copyGoogleObject _).expects(move.src, move.dest).returning(())
    (ioUtil.copyGoogleObject _)
      .expects(copy.src, copy.dest)
      .throwing(new IOException(msg))
    (ioUtil.writeGoogleObjectData _).expects(write.contents, write.dest).returning(())

    recoverToExceptionIf[IOException] {
      MoveExecutor
        .runPreUpsertOps(immutable.Seq(move, copy, write), ioUtil)
        .runWith(Sink.ignore)
    }.map { ex =>
      val sw = new StringWriter
      ex.printStackTrace(new PrintWriter(sw))
      sw.toString should include(msg)
    }
  }

  Seq(None, Some("the-new-basename")).foreach {
    it should behave like moveTest(_)
  }

  def moveTest(newBasename: Option[String]): Unit = {
    it should s"move cloud documents, with new basename $newBasename" in {
      val ioUtil = mock[IoUtil]
      val webClient = mock[ClioWebClient]

      (webClient
        .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
        .expects(CramIndex, theKey, false)
        .returning(Source.single(metadata))

      val paths = Seq
        .concat(metadata.cramPath, metadata.craiPath)
        .zip(Seq(CramExtensions.CramExtension, CramExtensions.CraiExtension))

      (ioUtil.isGoogleDirectory _).expects(destination).returning(true)
      paths.foreach {
        case (uri, ext) =>
          (ioUtil.isGoogleObject _).expects(uri).returning(true)

          val expectedDest =
            Metadata.findNewPathForMove(uri, destination, ext, newBasename)
          (ioUtil.copyGoogleObject _).expects(uri, expectedDest).returning(())
      }
      (ioUtil
        .deleteCloudObjects(_: immutable.Iterable[URI])(_: ExecutionContext))
        .expects(
          immutable.Iterable.concat(metadata.cramPath, metadata.craiPath),
          executionContext
        )
        .returning(Source.single(()))

      (webClient
        .upsert(_: Aux)(_: CramKey, _: CramMetadata, _: Boolean))
        .expects(CramIndex, theKey, metadata.moveInto(destination, newBasename), true)
        .returning(Source.single(id.asJson))

      val executor = new MoveExecutor(MoveCram(theKey, destination, newBasename))
      executor.execute(webClient, ioUtil).runWith(Sink.head).map { json =>
        json.as[UpsertId] should be(Right(id))
      }
    }
  }

  it should "not move on-prem documents" in {
    val executor =
      new MoveExecutor(MoveCram(theKey.copy(location = Location.OnPrem), destination))
    recoverToSucceededIf[UnsupportedOperationException] {
      executor.execute(stub[ClioWebClient], stub[IoUtil]).runWith(Sink.ignore)
    }
  }

  it should "fail if the destination isn't a cloud directory" in {
    val ioUtil = mock[IoUtil]
    (ioUtil.isGoogleDirectory _).expects(destination).returning(false)

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    recoverToSucceededIf[IllegalArgumentException] {
      executor.execute(stub[ClioWebClient], ioUtil).runWith(Sink.head)
    }
  }

  it should "fail if querying the server for existing metadata fails" in {
    val ioUtil = mock[IoUtil]
    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)

    val webClient = mock[ClioWebClient]
    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.failed(serverErr))

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    recoverToSucceededIf[ClioWebClient.FailedResponse] {
      executor.execute(webClient, ioUtil).runWith(Sink.head)
    }
  }

  it should "fail if trying to move an unregistered document" in {
    val ioUtil = mock[IoUtil]
    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)

    val webClient = mock[ClioWebClient]
    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.empty)

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    recoverToSucceededIf[IllegalStateException] {
      executor.execute(webClient, ioUtil).runWith(Sink.head)
    }
  }

  it should "fail if a document has no registered files to move" in {
    val ioUtil = mock[IoUtil]
    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)

    val empty = CramMetadata()

    val webClient = mock[ClioWebClient]
    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(empty))

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    recoverToSucceededIf[IllegalStateException] {
      executor.execute(webClient, ioUtil).runWith(Sink.head)
    }
  }

  it should "not fail if some files-to-move are already at the destination" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    val movedCram = metadata.cramPath.map(
      Metadata.findNewPathForMove(_, destination, CramExtensions.CramExtension)
    )

    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(metadata.copy(cramPath = movedCram)))

    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)

    val movedCrai = metadata.craiPath.map(
      Metadata.findNewPathForMove(_, destination, CramExtensions.CraiExtension)
    )
    metadata.craiPath.zip(movedCrai).foreach {
      case (src, dest) =>
        (ioUtil.isGoogleObject _).expects(src).returning(true)
        (ioUtil.copyGoogleObject _).expects(src, dest).returning(())
        (ioUtil
          .deleteCloudObjects(_: immutable.Iterable[URI])(_: ExecutionContext))
          .expects(immutable.Iterable(src), executionContext)
          .returning(Source.single(()))
    }

    (webClient
      .upsert(_: Aux)(_: CramKey, _: CramMetadata, _: Boolean))
      .expects(CramIndex, theKey, metadata.moveInto(destination), true)
      .returning(Source.single(id.asJson))

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    executor.execute(webClient, ioUtil).runWith(Sink.head).map { json =>
      json.as[UpsertId] should be(Right(id))
    }
  }

  it should "no-op if all files-to-move are already at the destination" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    val movedCram = metadata.cramPath.map(
      Metadata.findNewPathForMove(_, destination, CramExtensions.CramExtension)
    )
    val movedCrai = metadata.craiPath.map(
      Metadata.findNewPathForMove(_, destination, CramExtensions.CraiExtension)
    )

    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(metadata.copy(cramPath = movedCram, craiPath = movedCrai)))

    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    executor.execute(webClient, ioUtil).runWith(Sink.headOption).map { maybeJson =>
      maybeJson should be(None)
    }
  }

  it should "fail if on-prem paths are registered to a cloud document" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = Seq.concat(metadata.cramPath, metadata.craiPath)

    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)
    paths.foreach { uri =>
      (ioUtil.isGoogleObject _).expects(uri).returning(false)
    }

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    recoverToExceptionIf[IllegalStateException] {
      executor.execute(webClient, ioUtil).runWith(Sink.ignore)
    }.map { ex =>
      paths.foreach { uri =>
        ex.getMessage should include(uri.toString)
      }
      succeed
    }
  }

  it should "fail if pre-upsert IO operations fail" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = Seq
      .concat(metadata.cramPath, metadata.craiPath)
      .zip(Seq(CramExtensions.CramExtension, CramExtensions.CraiExtension))

    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)
    paths.foreach {
      case (uri, ext) =>
        (ioUtil.isGoogleObject _).expects(uri).returning(true)

        val expectedDest =
          Metadata.findNewPathForMove(uri, destination, ext)
        (ioUtil.copyGoogleObject _)
          .expects(uri, expectedDest)
          .throwing(new IOException(uri.toString))
    }

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    recoverToSucceededIf[RuntimeException] {
      executor.execute(webClient, ioUtil).runWith(Sink.ignore)
    }
  }

  it should "fail if the upsert to the server fails" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = Seq
      .concat(metadata.cramPath, metadata.craiPath)
      .zip(Seq(CramExtensions.CramExtension, CramExtensions.CraiExtension))

    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)
    paths.foreach {
      case (uri, ext) =>
        (ioUtil.isGoogleObject _).expects(uri).returning(true)

        val expectedDest =
          Metadata.findNewPathForMove(uri, destination, ext)
        (ioUtil.copyGoogleObject _).expects(uri, expectedDest).returning(())
    }

    (webClient
      .upsert(_: Aux)(_: CramKey, _: CramMetadata, _: Boolean))
      .expects(CramIndex, theKey, metadata.moveInto(destination), true)
      .returning(Source.failed(serverErr))

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    recoverToSucceededIf[RuntimeException] {
      executor.execute(webClient, ioUtil).runWith(Sink.ignore)
    }
  }

  it should "fail if post-upsert IO operations fail" in {
    val ioUtil = mock[IoUtil]
    val webClient = mock[ClioWebClient]

    (webClient
      .getMetadataForKey(_: Aux)(_: CramKey, _: Boolean))
      .expects(CramIndex, theKey, false)
      .returning(Source.single(metadata))

    val paths = Seq
      .concat(metadata.cramPath, metadata.craiPath)
      .zip(Seq(CramExtensions.CramExtension, CramExtensions.CraiExtension))

    (ioUtil.isGoogleDirectory _).expects(destination).returning(true)
    paths.foreach {
      case (uri, ext) =>
        (ioUtil.isGoogleObject _).expects(uri).returning(true)

        val expectedDest =
          Metadata.findNewPathForMove(uri, destination, ext)
        (ioUtil.copyGoogleObject _).expects(uri, expectedDest).returning(())
    }
    (ioUtil
      .deleteCloudObjects(_: immutable.Iterable[URI])(_: ExecutionContext))
      .expects(
        immutable.Iterable.concat(metadata.cramPath, metadata.craiPath),
        executionContext
      )
      .returning(Source.failed(new IOException("Nope")))

    (webClient
      .upsert(_: Aux)(_: CramKey, _: CramMetadata, _: Boolean))
      .expects(CramIndex, theKey, metadata.moveInto(destination), true)
      .returning(Source.single(id.asJson))

    val executor = new MoveExecutor(MoveCram(theKey, destination))
    recoverToSucceededIf[RuntimeException] {
      executor.execute(webClient, ioUtil).runWith(Sink.ignore)
    }
  }
}
