package org.broadinstitute.clio.client.dispatch

import io.circe.parser.parse
import org.broadinstitute.clio.client.commands.AddCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.{ExecutionContext, Future}

class AddExecutor[CI <: ClioIndex](addCommand: AddCommand[CI])
    extends Executor[UpsertId] {
  import addCommand.index.implicits._

  private[dispatch] val name: String = addCommand.index.name
  private[dispatch] val prettyKey = ClassUtil.formatFields(addCommand.key)

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[UpsertId] = {

    val location = addCommand.metadataLocation

    val parsedOrError = parse(IoUtil.readMetadata(location)).left.map { err =>
      new IllegalArgumentException(
        s"Could not parse contents of $location as JSON.",
        err
      )
    }

    val decodedOrError = parsedOrError
      .flatMap(_.as[addCommand.index.MetadataType])
      .left
      .map(
        err => new IllegalArgumentException(s"Invalid metadata given at $location.", err)
      )

    decodedOrError.fold(
      Future.failed, {
        addFiles(
          webClient,
          _,
          addCommand.force
        )
      }
    )
  }

  private def addFiles(
    client: ClioWebClient,
    newMetadata: addCommand.index.MetadataType,
    force: Boolean
  )(implicit ec: ExecutionContext): Future[UpsertId] = {

    client
      .upsert(addCommand.index)(addCommand.key, newMetadata, force)
      .recover {
        case ex =>
          throw new RuntimeException(
            s"An error occurred while adding the $name record in Clio.",
            ex
          )
      }
  }
}
