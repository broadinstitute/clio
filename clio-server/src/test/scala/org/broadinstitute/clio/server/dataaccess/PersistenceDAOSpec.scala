package org.broadinstitute.clio.server.dataaccess

import java.net.URI
import java.nio.file.Files
import java.time.OffsetDateTime

import akka.stream.scaladsl.Sink
import io.circe.parser._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  DocumentMock,
  ElasticsearchIndex
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.Future

class PersistenceDAOSpec
    extends TestKitSuite("PersistenceDAOSpec")
    with ModelAutoDerivation {
  behavior of "PersistenceDAO"

  implicit val index: ElasticsearchIndex[DocumentMock] = DocumentMock.index

  def initIndex(dao: PersistenceDAO): Future[Unit] =
    dao.initialize(Seq(index), "fake-version")

  it should "initialize top-level storage for indexed documents" in {
    val dao = new MemoryPersistenceDAO()
    val wgsPath = dao.rootPath.resolve(index.rootDir)

    Files.exists(wgsPath) should be(false)
    Files.isDirectory(wgsPath) should be(false)

    for {
      _ <- initIndex(dao)
    } yield {
      Files.exists(wgsPath) should be(true)
      Files.isDirectory(wgsPath) should be(true)
    }
  }

  it should "write metadata updates to storage using the upsertId" in {
    val dao = new MemoryPersistenceDAO()
    val document =
      DocumentMock.default.copy(
        mockFilePath = Some(URI.create("gs://the-file"))
      )
    val document2 = DocumentMock.default.copy(mockFieldDouble = Some(0.9876))

    for {
      _ <- initIndex(dao)
      _ <- dao.writeUpdate(document)
      _ <- dao.writeUpdate(document2)
    } yield {
      Seq(document, document2).foreach { doc =>
        val expectedPath =
          dao.rootPath.resolve(
            s"${index.currentPersistenceDir}/${ClioDocument.persistenceFilename(doc.upsertId)}"
          )

        Files.exists(expectedPath) should be(true)
        Files.isRegularFile(expectedPath) should be(true)

        parse(new String(Files.readAllBytes(expectedPath)))
          .flatMap(_.as[DocumentMock])
          .map(_ should be(doc))
          .toTry
          .get
      }
      succeed
    }
  }

  val testDocs: List[DocumentMock] = List.fill(25)(DocumentMock.default)
  (None :: testDocs.map(Some.apply)).foreach {
    it should behave like recoveryTest(testDocs, _)
  }

  // From StackOverflow: https://stackoverflow.com/a/11458240
  def cutIntoBuckets[A](xs: Seq[A], n: Int): Vector[Seq[A]] = {
    val m = xs.length
    val targets = (0 to n).map { x =>
      math.round((x.toDouble * m) / n).toInt
    }
    def snip(xs: Seq[A], ns: Seq[Int], got: Vector[Seq[A]]): Vector[Seq[A]] = {
      if (ns.length < 2) got
      else {
        val (i, j) = (ns.head, ns.tail.head)
        snip(xs.drop(j - i), ns.tail, got :+ xs.take(j - i))
      }
    }
    snip(xs, targets, Vector.empty)
  }

  def recoveryTest(
    documents: Seq[DocumentMock],
    lastKnown: Option[DocumentMock]
  ): Unit = {
    val description =
      lastKnown.fold("recover all upserts from GCS when Elasticsearch is empty") { d =>
        s"recover all upserts from GCS after last-known ID ${d.upsertId}"
      }

    it should description in {
      val dao = new MemoryPersistenceDAO()
      val now = OffsetDateTime.now()

      val days = Seq(now.minusYears(1L), now.minusMonths(1L), now.minusDays(1L), now)
      val docsByDay = days.zip(cutIntoBuckets(documents, days.size))

      for {
        _ <- initIndex(dao)
        _ <- Future.sequence {
          docsByDay.flatMap {
            case (day, docs) => docs.map(dao.writeUpdate(_, day))
          }
        }
        result <- dao.getAllSince(lastKnown.map(_.upsertId)).runWith(Sink.seq)
      } yield {
        val decodedResults = result.map(_.as[DocumentMock].fold(throw _, identity))
        decodedResults should contain theSameElementsInOrderAs documents.slice(
          lastKnown.fold(-1)(documents.indexOf) + 1,
          documents.size
        )
      }
    }
  }

  it should "fail if the last document in Elasticsearch isn't found in storage" in {
    val document = DocumentMock.default
    val dao = new MemoryPersistenceDAO()

    for {
      _ <- initIndex(dao)
      result <- recoverToSucceededIf[NoSuchElementException] {
        dao.getAllSince(Some(document.upsertId)).runWith(Sink.seq)
      }
    } yield {
      result
    }
  }

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
