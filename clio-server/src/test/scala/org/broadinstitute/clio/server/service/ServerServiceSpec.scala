package org.broadinstitute.clio.server.service

import java.net.URI
import java.time.OffsetDateTime

import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchFieldMapper,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.{ClioServerConfig, TestKitSuite}
import org.broadinstitute.clio.status.model.ClioStatus
import org.broadinstitute.clio.transfer.model.{
  ModelMockIndex,
  ModelMockKey,
  ModelMockMetadata
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.Future

class ServerServiceSpec
    extends TestKitSuite("ServerServiceSpec")
    with ModelAutoDerivation {
  behavior of "ServerService"

  def serverServiceWithMockDefaults(
    serverStatusDAO: ServerStatusDAO = new MockServerStatusDAO(),
    persistenceDAO: PersistenceDAO = new MockPersistenceDAO(),
    searchDAO: SearchDAO = new MockSearchDAO(),
    httpServerDAO: HttpServerDAO = new MockHttpServerDAO()
  ) = new ServerService(
    serverStatusDAO,
    persistenceDAO,
    searchDAO,
    httpServerDAO,
    ClioServerConfig.Version.value
  )

  it should "beginStartup" in {
    val serverService = serverServiceWithMockDefaults()
    serverService.beginStartup()
    succeed
  }

  it should "awaitShutdown" in {
    val serverService = serverServiceWithMockDefaults()
    serverService.awaitShutdown()
    succeed
  }

  it should "awaitShutdownInf" in {
    val serverService = serverServiceWithMockDefaults()
    serverService.awaitShutdownInf()
    succeed
  }

  it should "shutdownAndWait" in {
    val serverService = serverServiceWithMockDefaults()
    serverService.shutdownAndWait()
    succeed
  }

  it should "startup" in {
    val statusDAO = new MemoryServerStatusDAO()
    val persistenceDAO = new MemoryPersistenceDAO()
    val serverService = serverServiceWithMockDefaults(
      serverStatusDAO = statusDAO,
      persistenceDAO = persistenceDAO
    )
    serverService.startup().map { _ =>
      statusDAO.setCalls should be(
        Seq(ClioStatus.Starting, ClioStatus.Recovering, ClioStatus.Started)
      )
    }
  }

  it should "fail to start if persistence initialization fails" in {
    val statusDAO = new MemoryServerStatusDAO()
    val persistenceDAO = new FailingPersistenceDAO()
    val serverService = serverServiceWithMockDefaults(
      serverStatusDAO = statusDAO,
      persistenceDAO = persistenceDAO
    )

    recoverToSucceededIf[Exception] {
      serverService.startup()
    }.map { _ =>
      statusDAO.setCalls should be(Seq(ClioStatus.Starting))
    }
  }

  it should "fail to start if search initialization fails" in {
    val statusDAO = new MemoryServerStatusDAO()
    val searchDAO = new FailingSearchDAO()
    val serverService = serverServiceWithMockDefaults(
      serverStatusDAO = statusDAO,
      searchDAO = searchDAO
    )

    recoverToSucceededIf[Exception] {
      serverService.startup()
    }.map { _ =>
      statusDAO.setCalls should be(Seq(ClioStatus.Starting))
    }
  }

  it should "recover metadata from storage" in {
    val persistenceDAO = new MemoryPersistenceDAO()
    val searchDAO = new MemorySearchDAO()

    val serverService = serverServiceWithMockDefaults(
      persistenceDAO = persistenceDAO,
      searchDAO = searchDAO
    )

    val numDocs = 5
    val initInSearch = numDocs / 2
    val keyLong = 1L
    val keyString = "mock-key"
    val key = ModelMockKey(keyLong, keyString)
    val metadata = ModelMockMetadata(
      Some(1.0),
      Some(1),
      Some(OffsetDateTime.now()),
      Some(Seq.empty[String]),
      Some(Seq.empty[URI]),
      Some(DocumentStatus.Normal),
      Some('md5),
      Some(URI.create("/mock")),
      Some(1L)
    )
    val document = key.asJson
      .deepMerge(metadata.asJson)

    val index = new ElasticsearchIndex[ModelMockIndex](
      ModelMockIndex(),
      Map.empty[String, String].asJson,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

    var counter = 0
    val initStoredDocuments = Seq.fill(numDocs)({
      // This generation is done inside this block because UpsertIds and EntityIds need to be unique.
      counter = counter + 1
      document
        .deepMerge(
          Map(
            ElasticsearchIndex.UpsertIdElasticsearchName -> UpsertId.nextId()
          ).asJson
        )
        .deepMerge(
          Map(
            ElasticsearchIndex.EntityIdElasticsearchName -> s"$keyLong.$keyString-$counter"
          ).asJson
        )
    })
    val initSearchDocuments = initStoredDocuments.take(initInSearch)

    for {
      _ <- persistenceDAO.initialize(Seq(index), "fake-version")
      _ <- Future.sequence(
        initStoredDocuments.map(persistenceDAO.writeUpdate(_, index))
      )
      _ <- Future.sequence(
        initSearchDocuments.map(searchDAO.updateMetadata(_)(index))
      )
      _ <- serverService.recoverMetadata(index)
    } yield {
      val upsertedDocs = searchDAO.updateCalls
        .flatMap(_._1)
      upsertedDocs should contain theSameElementsAs initStoredDocuments
    }
  }

  it should "shutdown" in {
    val serverService = serverServiceWithMockDefaults()
    for {
      _ <- serverService.shutdown()
    } yield succeed
  }
}
