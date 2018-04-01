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
import org.broadinstitute.clio.util.ClassUtil

class AddExecutor[CI <: ClioIndex](addCommand: AddCommand[CI]) extends Executor {
  import addCommand.index.implicits._

  private[dispatch] val name: String = addCommand.index.name
  private[dispatch] val prettyKey = ClassUtil.formatFields(addCommand.key)

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    readMetadata(addCommand.metadataLocation, ioUtil).flatMapConcat { metadata =>
      webClient
        .upsert(addCommand.index)(addCommand.key, metadata, addCommand.force)
        .mapError {
          case ex =>
            new RuntimeException(
              s"An error occurred while adding the $name record in Clio.",
              ex
            )
        }
    }
  }

  private def readMetadata(
    location: URI,
    ioUtil: IoUtil
  ): Source[addCommand.index.MetadataType, NotUsed] = {
    Source
      .single(location)
      .map(ioUtil.readMetadata)
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
