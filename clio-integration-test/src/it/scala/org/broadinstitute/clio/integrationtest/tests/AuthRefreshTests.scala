package org.broadinstitute.clio.integrationtest.tests

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import akka.stream.scaladsl.Source
import com.google.auth.oauth2.TimeChangingChangeListener
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.EnvIntegrationSpec

import scala.concurrent.duration._
import scala.util.Success

trait AuthRefreshTests { self: EnvIntegrationSpec =>

  behavior of "Clio Client"

  it should "refresh the auth token after it expires" in {

    googleCredential.addChangeListener(
      new TimeChangingChangeListener(
        // set time back 59 minutes so we can get a token that expires in one minute
        ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(59).toInstant.toEpochMilli
      )
    )
    // First refresh triggers the change listener to change the clock
    googleCredential.refresh()
    // Second refresh gets a token using the new clock
    googleCredential.refresh()

    val expirationInstant =
      googleCredential.getAccessToken.getExpirationTime.toInstant
    val tokenTtl = ChronoUnit.SECONDS.between(Instant.now(), expirationInstant)

    // Query the schema endpoint every 15 seconds until the auth token expires, then once more.
    val tickInterval = 15.seconds
    val numTicks = (tokenTtl / tickInterval.toSeconds) + 1

    logger.info(
      s"Ticking $numTicks times (${tickInterval.toSeconds * numTicks} seconds)"
    )
    Source
      .tick(Duration.Zero, tickInterval, ())
      .take(numTicks)
      .runFoldAsync(succeed) { (_, _) =>
        runCollectJson(
          ClioCommand.queryWgsUbamName,
          "--project",
          "a non-existent project"
        )
        // Don't care about result, just want to be sure auth doesn't expire.
          .map(_ => succeed)
          .andThen {
            case Success(_) => logger.info(s"Sleeping $tickInterval...")
          }
      }
  }
}
