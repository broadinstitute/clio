package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentMock,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.Elastic4sAutoDerivation._

import com.sksamuel.elastic4s.Indexable
import com.sksamuel.elastic4s.circe._
import io.circe.parser._

import java.nio.file.Files
import java.time.OffsetDateTime

class PersistenceDAOSpec extends TestKitSuite("PersistenceDAOSpec") {
  behavior of "PersistenceDAO"

  val index: ElasticsearchIndex[DocumentMock] = DocumentMock.index

  it should "initialize top-level storage for indexed documents" in {
    val dao = new MemoryPersistenceDAO()
    val wgsPath = dao.rootPath.resolve(index.rootDir)

    Files.exists(wgsPath) should be(false)
    Files.isDirectory(wgsPath) should be(false)

    for {
      _ <- dao.initialize(index)
    } yield {
      Files.exists(wgsPath) should be(true)
      Files.isDirectory(wgsPath) should be(true)
    }
  }

  it should "write metadata updates to storage using the UUID" in {
    val dao = new MemoryPersistenceDAO()
    val document =
      DocumentMock.default.copy(mockFilePath = Some("gs://the-file"))
    val document2 = DocumentMock.default.copy(mockFieldDouble = Some(0.9876))

    for {
      _ <- dao.initialize(index)
      _ <- dao.writeUpdate(document, index)
      _ <- dao.writeUpdate(document2, index)
    } yield {
      Seq(document, document2).foreach { doc =>
        val expectedPath =
          dao.rootPath.resolve(
            s"${index.currentPersistenceDir}/${doc.upsertId}.json"
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
    val documents = (0L until (2L * half)).map(
      n =>
        DocumentMock.default.copy(
          mockKeyLong = n,
          mockFilePath = Some(s"gs://document-mock-key-$n")
      )
    )
    documents.toSet.size should be(documents.size)
    documents.map(_.upsertId).toSet.size should be(documents.size)
    val expected = documents.drop(half)
    val result = dao
      .initialize(index)
      .map(
        _ =>
          documents
            .map(document => dao.writeUpdate(document, index))
      )
      .flatMap(_ => dao.getAllSince(expected.headOption.map(_.upsertId), index))
    result.flatMap(_.toVector should be(expected.tail))
  }

  it should "return all documents from GCS if no latest UUID is given" in {
    val dao = new MemoryPersistenceDAO()
    val documents = (0L until 26L).map(
      n =>
        DocumentMock.default.copy(
          mockKeyLong = n,
          mockFilePath = Some(s"gs://document-mock-key-$n")
      )
    )
    documents.toSet.size should be(documents.size)
    documents.map(_.upsertId).toSet.size should be(documents.size)
    val expected = documents
    val result = dao
      .initialize(index)
      .map(
        _ =>
          documents
            .map(document => dao.writeUpdate(document, index))
      )
      .flatMap(_ => dao.getAllSince(None, index))
    result.flatMap(_.toVector should be(expected))
  }

  it should "fail if upsertId is found more than once" in {
    val document = DocumentMock.default
    val upsertId = document.upsertId
    val dao = new MemoryPersistenceDAO()

    val now = OffsetDateTime.now()
    val yesterday = now.minusDays(1L)
    val filename = s"${document.upsertId}.json"
    val indexable = implicitly[Indexable[DocumentMock]]

    for {
      _ <- dao.initialize(index)
      _ = Seq(yesterday, now).map { dt =>
        val dir = Files.createDirectories(
          dao.rootPath.resolve(
            s"${index.rootDir}/${dt.format(ElasticsearchIndex.dateTimeFormatter)}"
          )
        )
        Files.write(dir.resolve(filename), indexable.json(document).getBytes)
      }
      x <- recoverToExceptionIf[RuntimeException] {
        dao.getAllSince(Some(upsertId), index)
      }
    } yield {
      x.getMessage should include(s" files end with /$upsertId.json in ")
    }
  }

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
