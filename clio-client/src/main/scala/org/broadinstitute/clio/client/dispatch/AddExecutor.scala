package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.syntax.either._
import io.circe.Json
import io.circe.parser.parse
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

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    import Executor.SourceMonadOps

    for {
      metadata <- readMetadata(addCommand.metadataLocation, ioUtil)
      upsertId <- webClient.upsert(addCommand.index)(
        addCommand.key,
        metadata,
        addCommand.force
      )
    } yield {
      upsertId
    }
  }

  /**
    * Build a stream which, when pulled, will read JSON from a URI and decode it
    * into the metadata type associated with `addCommand`.
    */
  private def readMetadata(
    location: URI,
    ioUtil: IoUtil
  ): Source[addCommand.index.MetadataType, NotUsed] = {
    Source
      .single(location)
      .map(ioUtil.readFileData)
      .map {
        parse(_).valueOr { err =>
          throw new IllegalArgumentException(
            s"Could not parse contents of $location as JSON.",
            err
          )
        }
      }
      .map {
        _.as[addCommand.index.MetadataType].valueOr { err =>
          throw new IllegalArgumentException(s"Invalid metadata given at $location.", err)
        }
      }
  }
}
