package org.broadinstitute.clio.client.dispatch

import io.circe.parser.parse
import org.broadinstitute.clio.client.commands.{AddCommand, ClioCommand}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.{ExecutionContext, Future}

class AddExecutor[TI <: TransferIndex](addCommand: AddCommand[TI])
    extends Executor[UpsertId] {
  import addCommand.index.implicits._

  private[dispatch] val name: String = addCommand.index.name
  private[dispatch] val prettyKey = ClassUtil.formatFields(addCommand.key)

  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[UpsertId] = {

    val location = addCommand.metadataLocation
    val commandName = addCommand.index.commandName

    val parsedOrError = parse(IoUtil.readMetadata(location)).left.map { err =>
      new IllegalArgumentException(
        s"Could not parse contents of $location as JSON.",
        err
      )
    }

    val decodedOrError = parsedOrError
      .flatMap(_.as[addCommand.index.MetadataType])
      .left
      .map { err =>
        new IllegalArgumentException(
          s"Invalid metadata given at $location. Run the '${ClioCommand.getSchemaPrefix}$commandName' command to see the valid JSON for ${name}s.",
          err
        )
      }

    decodedOrError.fold(
      Future.failed(_), { decoded =>
        {
          for {
            existingMetadata <- if (!addCommand.force) {
              webClient.getMetadataForKey(addCommand.index)(addCommand.key)
            } else {
              Future.successful(None)
            }
            upsertResponse <- addFiles(
              webClient,
              decoded,
              existingMetadata
            ).fold(
              left => Future.failed(left),
              identity
            )
          } yield {
            upsertResponse
          }
        }
      }
    )
  }

  private def addFiles(
    client: ClioWebClient,
    newMetadata: addCommand.index.MetadataType,
    existingMetadata: Option[addCommand.index.MetadataType],
  )(implicit ec: ExecutionContext): Either[Throwable, Future[UpsertId]] = {
    val differences = existingMetadata.fold(Iterable.empty[(String, Any, Any)]) {
      diffMetadata(_, newMetadata)
    }
    for {
      upsertResponse <- Either.cond(
        differences.isEmpty,
        client
          .upsert(addCommand.index)(addCommand.key, newMetadata)
          .recover {
            case ex =>
              throw new RuntimeException(
                s"An error occurred while adding the $name record in Clio.",
                ex
              )
          }, {
          val diffs = differences.map {
            case (field, newVal, oldVal) =>
              s"Field: $field, Old value: $oldVal, New value: $newVal"
          }.mkString("\n")

          new IllegalArgumentException(
            s"""Adding this document will overwrite the following existing metadata:
               |$diffs
               |Use '--force' to overwrite the existing data.""".stripMargin
          )
        }
      )
    } yield {
      upsertResponse
    }
  }

  private def diffMetadata(
    existingMetadata: addCommand.index.MetadataType,
    newMetadata: addCommand.index.MetadataType
  ): Iterable[(String, Any, Any)] = {
    val mapper = new CaseClassMapper[addCommand.index.MetadataType]

    val existingMetadataValues =
      mapper
        .vals(existingMetadata)
        .asInstanceOf[Map[String, Option[Any]]]
        .filterNot(_._2.isEmpty)

    val newMetadataValues =
      mapper
        .vals(newMetadata)
        .asInstanceOf[Map[String, Option[Any]]]
        .filterNot(_._2.isEmpty)

    val differentFields =
      newMetadataValues.keySet
        .intersect(existingMetadataValues.keySet)
        .filterNot { field =>
          existingMetadataValues(field).equals(newMetadataValues(field))
        }

    for (key <- differentFields)
      yield
        (
          key,
          newMetadataValues(key),
          existingMetadataValues(key)
        )
  }
}
