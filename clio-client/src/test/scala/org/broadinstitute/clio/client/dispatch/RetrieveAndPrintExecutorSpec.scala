package org.broadinstitute.clio.client.dispatch

import akka.stream.scaladsl.{Sink, Source}
import better.files.File
import io.circe.Json
import io.circe.jawn.JawnParser
import io.circe.literal._
import io.circe.syntax._
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.{
  GetServerHealth,
  GetServerVersion,
  QueryUbam,
  RawQueryUbam
}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.status.model.{
  ClioStatus,
  SearchStatus,
  StatusInfo,
  VersionInfo
}
import org.broadinstitute.clio.transfer.model.UbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamQueryInput}
import org.broadinstitute.clio.util.model.Location
import org.scalamock.scalatest.AsyncMockFactory

import scala.collection.{immutable, mutable}

class RetrieveAndPrintExecutorSpec extends BaseClientSpec with AsyncMockFactory {
  behavior of "RetrieveAndPrintExecutor"

  private val parser = new JawnParser

  it should "retrieve and print server health" in {
    val health = StatusInfo(ClioStatus.Started, SearchStatus.Error)

    val webClient = mock[ClioWebClient]
    (webClient.getClioServerHealth _).expects().returning(Source.single(health.asJson))

    val stdout = mutable.StringBuilder.newBuilder
    val executor = new RetrieveAndPrintExecutor(GetServerHealth, { s =>
      stdout.append(s)
      ()
    })

    executor.execute(webClient, stub[IoUtil]).runWith(Sink.head).map { json =>
      json.as[StatusInfo] should be(Right(health))
      parser.decode[StatusInfo](stdout.toString()) should be(Right(health))
    }
  }

  it should "retrieve and print server version" in {
    val version = VersionInfo("the-version")

    val webClient = mock[ClioWebClient]
    (webClient.getClioServerVersion _).expects().returning(Source.single(version.asJson))

    val stdout = mutable.StringBuilder.newBuilder
    val executor = new RetrieveAndPrintExecutor(GetServerVersion, { s =>
      stdout.append(s)
      ()
    })

    executor.execute(webClient, stub[IoUtil]).runWith(Sink.head).map { json =>
      json.as[VersionInfo] should be(Right(version))
      parser.decode[VersionInfo](stdout.toString()) should be(Right(version))
    }
  }

  Seq(true, false).foreach {
    it should behave like queryTest(_)
  }

  def queryTest(includeDeleted: Boolean): Unit = {
    it should s"retrieve and print query results with includeDeleted=$includeDeleted" in {
      val query = UbamQueryInput(flowcellBarcode = Some("abcd"))
      val keys = immutable.Seq.tabulate(10) { i =>
        UbamKey(
          flowcellBarcode = "abcd",
          lane = i,
          libraryName = "efgh",
          location = Location.GCP
        )
      }

      val webClient = mock[ClioWebClient]
      // Type annotations needed for scalamockery.
      (
        webClient
          .simpleQuery(_: ClioWebClient.QueryAux[UbamQueryInput])(
            _: UbamQueryInput,
            _: Boolean,
            _: Boolean
          )
        )
        .expects(UbamIndex, query, includeDeleted, includeDeleted)
        .returning(Source(keys.map(_.asJson)))

      val stdout = mutable.StringBuilder.newBuilder
      val executor = new RetrieveAndPrintExecutor(QueryUbam(query, includeDeleted), { s =>
        stdout.append(s)
        ()
      })

      executor.execute(webClient, stub[IoUtil]).runWith(Sink.seq).map { jsons =>
        jsons.map(_.as[UbamKey]) should contain theSameElementsAs keys.map(Right(_))
        parser.decode[Seq[UbamKey]](stdout.toString()) should be(Right(keys))
      }
    }
  }

  it should "retrieve and print raw query results" in {
    val rawQuery = json"""{ "key" : "valid json" }"""
    val keys = immutable.Seq.tabulate(10) { i =>
      UbamKey(
        flowcellBarcode = "abcd",
        lane = i,
        libraryName = "efgh",
        location = Location.GCP
      )
    }

    val webClient = mock[ClioWebClient]
    (webClient
      .query(_: UbamIndex.type)(_: Json, _: Boolean))
      .expects(UbamIndex, rawQuery, true)
      .returning(Source(keys.map(_.asJson)))

    File
      .temporaryFile()
      .map { tempFile =>
        tempFile.write(rawQuery.noSpaces)
        val stdout = mutable.StringBuilder.newBuilder
        val executor = new RetrieveAndPrintExecutor(RawQueryUbam(tempFile), { s =>
          stdout.append(s)
          ()
        })
        executor.execute(webClient, stub[IoUtil]).runWith(Sink.seq).map { jsons =>
          jsons.map(_.as[UbamKey]) should contain theSameElementsAs keys.map(Right(_))
          parser.decode[Seq[UbamKey]](stdout.toString()) should be(Right(keys))
        }
      }
      .get
  }
}
