package org.broadinstitute.clio.server.dataaccess

import java.net.URI
import java.time.OffsetDateTime

import akka.stream.scaladsl.Sink
import io.circe.Json
import io.circe.syntax._
import io.circe.parser._
import org.broadinstitute.clio.transfer.model.{ModelMockIndex, ModelMockKey, ModelMockMetadata}
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{ElasticsearchFieldMapper, ElasticsearchIndex}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.Future

class PersistenceDAOSpec
    extends TestKitSuite("PersistenceDAOSpec")
    with ModelAutoDerivation {
  behavior of "PersistenceDAO"

  implicit val index: ElasticsearchIndex[ModelMockIndex] = new ElasticsearchIndex(
    "mock",
    ModelMockIndex(),
    ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
  )

  def initIndex(dao: PersistenceDAO): Future[Unit] =
    dao.initialize(Seq(index), "fake-version")

  it should "initialize top-level storage for indexed documents" in {
    val dao = new MemoryPersistenceDAO()
    val wgsPath = dao.rootPath / index.rootDir

    wgsPath.exists should be(false)
    wgsPath.isDirectory should be(false)

    for {
      _ <- initIndex(dao)
    } yield {
      wgsPath.exists should be(true)
      wgsPath.isDirectory should be(true)
    }
  }

  it should "write metadata updates to storage using the upsertId" in {
    val dao = new MemoryPersistenceDAO()

    val key1 = ModelMockKey(1L, "mock-key-1")
    val metadata1 = ModelMockMetadata(None, None, None, None, None, None)
    val document1 = key1.asJson.deepMerge(metadata1.asJson).deepMerge(Map("mockFilePath" -> Some(URI.create("gs://the-file"))).asJson)

    val key2 = ModelMockKey(2L, "mock-key-2")
    val metadata2 = ModelMockMetadata(Some(0.9876), None, None, None, None, None)
    val document2 = key2.asJson.deepMerge(metadata2.asJson)

    for {
      _ <- initIndex(dao)
      _ <- dao.writeUpdate(document1, index)
      _ <- dao.writeUpdate(document2, index)
    } yield {
      Seq(document1, document2).foreach { doc =>
        val expectedPath =
          dao.rootPath / s"${index.currentPersistenceDir}/${ElasticsearchIndex.getUpsertId(doc)
            .persistenceFilename}"

        expectedPath.exists should be(true)
        expectedPath.isRegularFile should be(true)

        parse(expectedPath.contentAsString)
          .map(_ should be(doc))
          .toTry
          .get
      }
      succeed
    }
  }

  val mockKeyJson = ModelMockKey(0L, "mock").asJson
  val mockMetadataJson = ModelMockMetadata(None, None, None, None, None, None).asJson
  val testDocs: List[Json] = List.fill(25)(mockKeyJson.deepMerge(mockMetadataJson))
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
    documents: Seq[Json],
    lastKnown: Option[Json]
  ): Unit = {
    val description =
      lastKnown.fold("recover all upserts from GCS when Elasticsearch is empty") { d =>
        s"recover all upserts from GCS after last-known ID ${ElasticsearchIndex.getUpsertId(d).id}"
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
            case (day, docs) => docs.map(dao.writeUpdate(_, index, day))
          }
        }
        result <- dao.getAllSince(lastKnown.map(ElasticsearchIndex.getUpsertId(_))).runWith(Sink.seq)
      } yield {
        result should contain theSameElementsInOrderAs documents.slice(
          lastKnown.fold(-1)(documents.indexOf) + 1,
          documents.size
        )
      }
    }
  }

  it should "fail if the last document in Elasticsearch isn't found in storage" in {
    val dao = new MemoryPersistenceDAO()

    for {
      _ <- initIndex(dao)
      result <- recoverToSucceededIf[NoSuchElementException] {
        dao.getAllSince(Some(UpsertId.nextId())).runWith(Sink.seq)
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
