package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.admin.UpdateIndexLevelSettingsRequest
import com.sksamuel.elastic4s.http.{ElasticRequest, HttpEntity}
import com.sksamuel.elastic4s.settings.UpdateSettingsRequest
import io.circe.parser._
import org.apache.http.HttpHost
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.transfer.model.HealthStatus
import org.scalatest._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

/**
  * Runs a suite of tests on an Elasticsearch docker image.
  *
  * Subclasses should start with a spec that calls initialize().
  *
  * @param actorSystemName The name of the actor system.
  */
abstract class AbstractElasticsearchDAOSpec(actorSystemName: String)
    extends TestKitSuite(actorSystemName)
    with ElasticsearchContainer { this: AsyncTestSuite =>

  private lazy val httpHost =
    new HttpHost(elasticsearchContainerIpAddress, elasticsearchPort)
  final lazy val httpElasticsearchDAO = new HttpElasticsearchDAO(Seq(httpHost))

  override protected def afterAll(): Unit = {
    // Not using regular .close within afterAll. The execution context provided by scalatest doesn't seem to run here.
    httpElasticsearchDAO.closeClient()
    super.afterAll()
  }

  protected def initialize(): Future[Assertion] = {
    // Set *all* existing index replicas to 0, including elasticsearch's internal indexes
    val entity = HttpEntity(
      """|{
         |  "index" : {
         |    "number_of_replicas" : 0
         |  }
         |}""".stripMargin
    )

    val params = Map.empty[String, String]

    /**
      * Use the internal Elasticsearch REST client to make a call to settings and disable all of index replication.
      *
      * https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
      *
      * @return A Unit if the disabling was successful, or an error.
      */
    def performDisableReplicas(): Future[Unit] = {
      val futureResponse =
        httpElasticsearchDAO.elasticClient.execute(ElasticRequest("PUT", "/_settings", entity))
      futureResponse map { response =>
        val json =
          response.entity.fold(fail("No JSON returned from Elasticsearch"))(_.content)
        val jsonDecode = decode[Map[String, Boolean]](json)
        val jsonMap = jsonDecode.right.getOrElse(
          fail(s"Unexpected response to setting replicas to 0: $json")
        )
        if (!jsonMap("acknowledged"))
          fail("Setting replicas to 0 was not acknowledged")
      }
    }

    /**
      * Repeatedly retry disabling the index replicas.
      *
      * @param count The number of attempts.
      * @param sleep The time between attempts.
      * @return A Unit if the disabling was successful, or an error.
      */
    def disableExistingReplicas(count: Int, sleep: Duration): Future[Unit] = {
      performDisableReplicas() transformWith {
        case Success(_) =>
          Future.successful(())
        case other if count == 0 =>
          Future.failed(new RuntimeException(s"Timeout with status: $other"))
        case _ =>
          Thread.sleep(sleep.toMillis)
          disableExistingReplicas(count - 1, sleep)
      }
    }

    /**
      * Repeatedly retry waiting for the cluster to go green. If the cluster returns yellow, this function will attempt
      * to disable the index replication and check for green again.
      *
      * @param count The number of attempts.
      * @param sleep The time between attempts.
      * @return A Unit if the disabling was successful, or an error.
      */
    def waitForGreen(count: Int, sleep: Duration): Future[Unit] = {
      httpElasticsearchDAO.getClusterHealth transformWith {
        case Success(response)
            if HealthStatus.withNameInsensitive(response.status) == HealthStatus.Green =>
          Future.successful(())
        case Success(response)
            if HealthStatus.withNameInsensitive(response.status) == HealthStatus.Yellow && count > 0 =>
          // If we got a yellow, we missed disabling one of the indexes. Try again.
          performDisableReplicas flatMap { _ =>
            waitForGreen(count - 1, sleep)
          }
        case other if count == 0 =>
          Future.failed(new RuntimeException(s"Timeout with status: $other"))
        case _ =>
          Thread.sleep(sleep.toMillis)
          waitForGreen(count - 1, sleep)
      }
    }

    for {
      _ <- disableExistingReplicas(20, 3.seconds)
      _ <- waitForGreen(20, 3.seconds)
    } yield succeed
  }
}
