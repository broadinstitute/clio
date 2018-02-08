package org.broadinstitute.clio.server.dataaccess

import java.net.URI
import java.nio.file.Files
import java.time.OffsetDateTime

import akka.stream.scaladsl.Sink
import io.circe.parser._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentMock,
  Elastic4sAutoDerivation,
  ElasticsearchIndex
}

class PersistenceDAOSpec
    extends TestKitSuite("PersistenceDAOSpec")
    with Elastic4sAutoDerivation {
  behavior of "PersistenceDAO"

  implicit val index: ElasticsearchIndex[DocumentMock] = DocumentMock.index

  it should "initialize top-level storage for indexed documents" in {
    val dao = new MemoryPersistenceDAO()
    val wgsPath = dao.rootPath.resolve(index.rootDir)

    Files.exists(wgsPath) should be(false)
    Files.isDirectory(wgsPath) should be(false)

    for {
      _ <- dao.initialize(Seq(index))
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
      _ <- dao.initialize(Seq(index))
      _ <- dao.writeUpdate(document)
      _ <- dao.writeUpdate(document2)
    } yield {
      Seq(document, document2).foreach { doc =>
        val expectedPath =
          dao.rootPath.resolve(
            s"${index.currentPersistenceDir}/${doc.persistenceFilename}"
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
  def cut[A](xs: Seq[A], n: Int): Vector[Seq[A]] = {
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
      val docsByDay = cut(documents, 4)

      for {
        _ <- dao.initialize(Seq(index))
        _ = days.zip(docsByDay).foreach {
          case (day, docs) =>
            val dir = Files.createDirectories(
              dao.rootPath.resolve(
                s"${index.rootDir}/${day.format(ElasticsearchIndex.dateTimeFormatter)}"
              )
            )
            docs.foreach { document =>
              Files.write(
                dir.resolve(document.persistenceFilename),
                index.indexable.json(document).getBytes
              )
            }
        }
        result <- dao.getAllSince[DocumentMock](lastKnown).runWith(Sink.seq)
      } yield {
        result should contain theSameElementsInOrderAs documents.slice(
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
      _ <- dao.initialize(Seq(index))
      result <- recoverToSucceededIf[NoSuchElementException] {
        dao.getAllSince[DocumentMock](Some(document)).runWith(Sink.seq)
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
