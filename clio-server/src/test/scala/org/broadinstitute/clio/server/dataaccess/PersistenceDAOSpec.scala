package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentMock,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.Elastic4sAutoDerivation._
import com.sksamuel.elastic4s.circe._
import io.circe.parser._
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}
import java.nio.file.Files
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit

class PersistenceDAOSpec
    extends TestKit(ActorSystem("PeristenceDAOSpec"))
    with AsyncFlatSpecLike
    with BeforeAndAfterAll
    with Matchers {
  behavior of "PersistenceDAO"

  private implicit val materializer = ActorMaterializer()

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
          mockFilePath = Some(s"gs://document-mock-key-${n}")
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
      .flatMap(_ => dao.getAllSince(expected.head.upsertId, index))
    result.flatMap(_.toVector should be(expected))
  }

  it should "fail unless upsertId is found exactly once" in {
    val upsertId = UUID.randomUUID()
    val dao = new MemoryPersistenceDAO()
    for {
      _ <- dao.initialize(index)
      _ <- dao.writeUpdate(DocumentMock.default, index)
      x <- recoverToExceptionIf[RuntimeException] {
        dao.getAllSince(upsertId, index)
      }
    } yield {
      x.getMessage should include(s" files end with /${upsertId}.json in ")
    }
  }

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
