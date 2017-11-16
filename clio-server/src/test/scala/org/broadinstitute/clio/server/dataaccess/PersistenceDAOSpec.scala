package org.broadinstitute.clio.server.dataaccess

import java.net.URI
import java.nio.file.Files
import java.time.OffsetDateTime

import akka.stream.scaladsl.Sink
import com.sksamuel.elastic4s.Indexable
import com.sksamuel.elastic4s.circe._
import io.circe.parser._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentMock,
  Elastic4sAutoDerivation,
  ElasticsearchIndex
}

import scala.concurrent.Future

class PersistenceDAOSpec
    extends TestKitSuite("PersistenceDAOSpec")
    with Elastic4sAutoDerivation {
  behavior of "PersistenceDAO"

  val index: ElasticsearchIndex[DocumentMock] = DocumentMock.index

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
      _ <- dao.writeUpdate(document, index)
      _ <- dao.writeUpdate(document2, index)
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

  it should "return metadata documents from GCS in order" in {
    val dao = new MemoryPersistenceDAO()
    val half = 23
    val documents = Seq.tabulate(2 * half) { n =>
      DocumentMock.default.copy(
        mockKeyLong = n.toLong,
        mockFilePath = Some(URI.create(s"gs://document-mock-key-$n"))
      )
    }
    documents.toSet.size should be(documents.size)
    documents.map(_.upsertId).toSet.size should be(documents.size)
    val expected = documents.drop(half)
    for {
      _ <- dao.initialize(Seq(index))
      _ <- Future.sequence(documents.map(dao.writeUpdate(_, index)))
      result <- dao.getAllSince(expected.headOption, index).runWith(Sink.seq)
    } yield {
      result should be(expected.tail)
    }
  }

  it should "return all documents from GCS if no latest upsertId is given" in {
    val dao = new MemoryPersistenceDAO()
    val documents = Seq.tabulate(26) { n =>
      DocumentMock.default.copy(
        mockKeyLong = n.toLong,
        mockFilePath = Some(URI.create(s"gs://document-mock-key-$n"))
      )
    }
    documents.toSet.size should be(documents.size)
    documents.map(_.upsertId).toSet.size should be(documents.size)
    val expected = documents
    for {
      _ <- dao.initialize(Seq(index))
      _ <- Future.sequence(documents.map(dao.writeUpdate(_, index)))
      result <- dao.getAllSince(None, index).runWith(Sink.seq)
    } yield {
      result should be(expected)
    }
  }

  it should "stop searching storage when it finds the document with the last-known upsert ID" in {
    val document = DocumentMock.default
    val dao = new MemoryPersistenceDAO()

    val now = OffsetDateTime.now()
    val yesterday = now.minusDays(1L)
    val indexable = implicitly[Indexable[DocumentMock]]

    for {
      _ <- dao.initialize(Seq(index))
      _ = Seq(yesterday, now).map { dt =>
        val dir = Files.createDirectories(
          dao.rootPath.resolve(
            s"${index.rootDir}/${dt.format(ElasticsearchIndex.dateTimeFormatter)}"
          )
        )
        Files.write(
          dir.resolve(document.persistenceFilename),
          indexable.json(document).getBytes
        )
      }
      result <- dao.getAllSince(Some(document), index).runWith(Sink.seq)
    } yield {
      result shouldBe empty
    }
  }

  it should "fail if the last document in Elasticsearch isn't found in storage" in {
    val document = DocumentMock.default
    val dao = new MemoryPersistenceDAO()

    for {
      _ <- dao.initialize(Seq(index))
      result <- recoverToSucceededIf[NoSuchElementException] {
        dao.getAllSince(Some(document), index).runWith(Sink.seq)
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
