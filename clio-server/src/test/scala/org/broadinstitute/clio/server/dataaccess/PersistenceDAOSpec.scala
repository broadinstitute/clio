package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  DocumentMock,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.Elastic4sAutoDerivation._
import org.broadinstitute.clio.server.dataaccess.util.ClioUUIDGenerator

import com.sksamuel.elastic4s.circe._
import io.circe.parser._
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.collection.JavaConverters._

import java.nio.file.Files

class PersistenceDAOSpec extends AsyncFlatSpec with Matchers {
  behavior of "PersistenceDAO"

  val index: ElasticsearchIndex[DocumentMock] = DocumentMock.index

  it should "initialize top-level storage for indexed documents" in {
    val dao = new JimfsPersistenceDAO()
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
    val dao = new JimfsPersistenceDAO()
    val document = DocumentMock(
      clioId = ClioUUIDGenerator.getUUID(),
      mockKeyLong = 1234L,
      mockKeyString = "the key",
      mockFilePath = Some("gs://the-file")
    )
    val document2 = document.copy(
      clioId = ClioUUIDGenerator.getUUID(),
      mockFilePath = None,
      mockFieldDouble = Some(0.9876)
    )

    for {
      _ <- dao.initialize(index)
      _ <- dao.writeUpdate(document, index)
      _ <- dao.writeUpdate(document2, index)
    } yield {
      Seq(document, document2).foldLeft(succeed) {
        case (_, doc) => {
          val expectedPath =
            dao.currentPathForIndex(index).resolve(s"${doc.clioId}.json")

          Files.exists(expectedPath) should be(true)
          Files.isRegularFile(expectedPath) should be(true)

          parse(Files.readAllLines(expectedPath).asScala.mkString)
            .flatMap(_.as[DocumentMock])
            .map(_ should be(doc))
            .toTry
            .get
        }
      }
    }
  }
}
