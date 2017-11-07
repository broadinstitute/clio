package org.broadinstitute.clio.integrationtest.tests

import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.transfer.model.WgsUbamIndex

import scala.concurrent.duration._
import scala.util.Success

trait AuthRefreshTests { self: BaseIntegrationSpec =>

  behavior of "Clio Client"

  it should "refresh the auth token after it expires" in {
    /* Query the wgs-ubam schema every 3 seconds for 1.5 hours. */
    val tickInterval = 3.seconds

    Source
      .tick(Duration.Zero, tickInterval, ())
      .take((1.5.hours / tickInterval).round)
      .runFoldAsync(succeed) { (_, _) =>
        runClientGetJsonAs[Json](ClioCommand.getWgsUbamSchemaName)
          .map(_ should be(WgsUbamIndex.jsonSchema))
          .andThen {
            case Success(_) => logger.info(s"Sleeping $tickInterval...")
          }
      }
  }
}
