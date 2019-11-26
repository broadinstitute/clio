package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.syntax.show._
import io.circe.Json
import org.broadinstitute.clio.client.commands.RelinquishCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex

/**
  * Executor for "relinquish" commands, which updates record in Clio to mark
  * them as `ExternallyHosted`. This tells us that we no longer control the
  * files.
  */
class RelinquishExecutor[CI <: ClioIndex](relinquishCommand: RelinquishCommand[CI])
    extends Executor {

  import relinquishCommand.index.implicits._

  private val prettyKey = relinquishCommand.key.show
  val name: String = relinquishCommand.index.name

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    import Executor.SourceMonadOps

    for {
      existingMetadata <- checkPreconditions(webClient)
      upsertId <- webClient
        .upsert(relinquishCommand.index)(
          relinquishCommand.key,
          existingMetadata.markExternallyHosted(relinquishCommand.note),
          // Always force because we're purposefully overwriting document status.
          force = true
        )
        .mapError {
          case ex =>
            new RuntimeException(
              s"Failed to relinquish the $name record for $prettyKey in Clio.",
              ex
            )
        }
    } yield {
      logger.info(s"Successfully relinquished record and files for $prettyKey.")
      upsertId
    }
  }

  /**
    * Build a stream which, when pulled, will ensure that the command being executed
    * refers to a cloud document already registered in the clio-server, emitting the
    * existing metadata on success.
    */
  private def checkPreconditions(
    webClient: ClioWebClient
  ): Source[relinquishCommand.index.MetadataType, NotUsed] = {
    val metadata: Source[relinquishCommand.index.MetadataType, NotUsed] =
      webClient.getMetadataForKey(relinquishCommand.index)(
        relinquishCommand.key,
        includeDeleted = false
      )

    metadata.orElse {
      // Has to be lazy because `orElse` eagerly fails as soon as either branch fails.
      Source.lazily { () =>
        Source.failed(
          new IllegalStateException(
            s"No $name found in Clio for $prettyKey, nothing to relinquish."
          )
        )
      }
    }
  }
}
