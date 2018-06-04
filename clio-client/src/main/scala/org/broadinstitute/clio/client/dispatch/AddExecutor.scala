package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.syntax.show._
import io.circe.Json
import org.broadinstitute.clio.client.commands.AddCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex

/**
  * Executor for "add" commands, which read some metadata from disk and upsert
  * it to the clio-server, optionally overwriting existing fields.
  */
class AddExecutor[CI <: ClioIndex](addCommand: AddCommand[CI]) extends Executor {

  import addCommand.index.implicits._

  private val prettyKey = addCommand.key.show

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    import Executor.SourceMonadOps

    for {
      metadata <- ioUtil.readMetadata(addCommand.index)(addCommand.metadataLocation)
      upsertId <- webClient.upsert(addCommand.index)(
        addCommand.key,
        metadata,
        addCommand.force
      )
    } yield {
      logger.info(s"Successfully updated record for $prettyKey.")
      upsertId
    }
  }
}
