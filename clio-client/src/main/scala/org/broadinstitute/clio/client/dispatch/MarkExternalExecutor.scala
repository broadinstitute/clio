package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.syntax.show._
import io.circe.Json
import org.broadinstitute.clio.client.commands.MarkExternalCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.model.DocumentStatus

/**
  * Executor for "make-external" commands, which updates record in Clio to mark
  * them as `External`. This tells us that we no longer control the
  * files.
  */
class MarkExternalExecutor[CI <: ClioIndex](markExternalCommand: MarkExternalCommand[CI])
    extends Executor {

  import markExternalCommand.index.implicits._

  private val prettyKey = markExternalCommand.key.show
  val name: String = markExternalCommand.index.name

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    import Executor.SourceMonadOps

    for {
      existingMetadata <- checkPreconditions(webClient)
      upsertId <- webClient
        .upsert(markExternalCommand.index)(
          markExternalCommand.key,
          existingMetadata
            .changeStatus(DocumentStatus.External, markExternalCommand.note),
          // Always force because we're purposefully overwriting document status.
          force = true
        )
        .mapError {
          case ex =>
            new RuntimeException(
              s"Failed to mark the $name record for $prettyKey as External in Clio.",
              ex
            )
        }
    } yield {
      logger.info(s"Successfully marked record and files for $prettyKey as External.")
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
  ): Source[markExternalCommand.index.MetadataType, NotUsed] = {
    val metadata: Source[markExternalCommand.index.MetadataType, NotUsed] =
      webClient.getMetadataForKey(markExternalCommand.index)(
        markExternalCommand.key,
        includeDeleted = false
      )

    metadata.orElse {
      // Has to be lazy because `orElse` eagerly fails as soon as either branch fails.
      Source.lazily { () =>
        Source.failed(
          new IllegalStateException(
            s"No $name found in Clio for $prettyKey, nothing to mark as external."
          )
        )
      }
    }
  }
}
