package org.broadinstitute.clio.integrationtest.tests

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.EnvIntegrationSpec
import org.broadinstitute.clio.transfer.model.WgsUbamIndex

import scala.concurrent.duration._
import scala.util.Success

trait AuthRefreshTests { self: EnvIntegrationSpec =>

  behavior of "Clio Client"

  it should "refresh the auth token after it expires" in {
    googleCredential.refresh()

    val expirationInstant =
      googleCredential.getAccessToken.getExpirationTime.toInstant
    val tokenTtl = ChronoUnit.SECONDS.between(Instant.now(), expirationInstant)

    // Query the schema endpoint every 3 seconds until the auth token expires, then once more.
    val tickInterval = 3.seconds
    val numTicks = (tokenTtl / tickInterval.toSeconds) + 1

    logger.info(
      s"Ticking $numTicks times (${tickInterval.toSeconds * numTicks} seconds)"
    )
    Source
      .tick(Duration.Zero, tickInterval, ())
      .take(numTicks)
      .runFoldAsync(succeed) { (_, _) =>
        runClientGetJsonAs[Json](ClioCommand.getWgsUbamSchemaName)
          .map(_ should be(WgsUbamIndex.jsonSchema))
          .andThen {
            case Success(_) => logger.info(s"Sleeping $tickInterval...")
          }
      }
  }
}
