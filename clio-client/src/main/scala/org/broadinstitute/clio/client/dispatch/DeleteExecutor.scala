package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.syntax.either._
import io.circe.Json
import org.broadinstitute.clio.client.commands.DeleteCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.model.Location

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class DeleteExecutor[CI <: ClioIndex](deleteCommand: DeleteCommand[CI])(
  implicit ec: ExecutionContext
) extends Executor {

  private val prettyKey = ClassUtil.formatFields(deleteCommand.key)
  val name: String = deleteCommand.index.name

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    if (!deleteCommand.key.location.equals(Location.GCP)) {
      Source.failed(
        new UnsupportedOperationException(
          s"Only cloud ${name}s are supported at this time."
        )
      )
    } else {
      webClient
        .getMetadataForKey(deleteCommand.index)(deleteCommand.key, includeDeleted = false)
        .orElse {
          // Has to be lazy because `orElse` eagerly fails as soon as either branch fails.
          Source.lazily { () =>
            Source.failed(
              new IllegalStateException(
                s"No $name found in Clio for $prettyKey, nothing to delete."
              )
            )
          }
        }
        .flatMapConcat { metadata =>
          val pathsToDelete = metadata.pathsToDelete.to[immutable.Iterable]
          checkPaths(pathsToDelete, ioUtil).flatMapConcat {
            ioUtil.deleteCloudObjects(_).mapError {
              case ex =>
                new RuntimeException(
                  s"""Errors encountered while deleting files for $prettyKey.
                       |The record for $prettyKey still exists in Clio.""".stripMargin,
                  ex
                )
            }
          }.flatMapConcat { _ =>
            webClient
              .upsert(deleteCommand.index)(
                deleteCommand.key,
                metadata.markDeleted(deleteCommand.note),
                // Always force because we're purposefully overwriting document status.
                force = true
              )
              .mapError {
                case ex =>
                  new RuntimeException(
                    s"""Failed to delete the $name record for $prettyKey in Clio.
                       |The associated files have been deleted in the cloud, but Clio doesn't know.
                       |Try re-running this command with '--force'.""".stripMargin,
                    ex
                  )
              }
          }
        }
    }
  }

  private def checkPaths(
    paths: immutable.Iterable[URI],
    ioUtil: IoUtil
  ): Source[immutable.Iterable[URI], NotUsed] = {
    Source[URI](paths)
      .mapAsync(paths.size) { path =>
        if (!ioUtil.isGoogleObject(path)) {
          Future.successful(
            Left(
              new IllegalStateException(
                s"Inconsistent state detected, non-cloud path '$path' registered to $prettyKey"
              )
            )
          )
        } else {
          Future(Either.catchNonFatal(ioUtil.googleObjectExists(path))).map { attempt =>
            attempt.flatMap { doesExist =>
              if (doesExist) {
                Right(Some(path))
              } else {
                val msg =
                  s"'$path' associated with $prettyKey does not exist in the cloud."
                if (deleteCommand.force) {
                  logger.warn(msg)
                  Right(None)
                } else {
                  Left(new IllegalStateException(msg))
                }
              }
            }
          }
        }
      }
      .fold(Right(Seq.empty[URI]): Either[Seq[Throwable], Seq[URI]]) {
        case (Right(ps), Right(maybePath)) => Right(ps ++ maybePath.toIterable)
        case (Right(_), Left(err))         => Left(Seq(err))
        case (Left(errs), Right(_))        => Left(errs)
        case (Left(errs), Left(err))       => Left(errs :+ err)
      }
      .flatMapConcat {
        _.fold(
          errs => Source.failed(new IllegalStateException(errs.mkString(", "))),
          paths => Source.single(paths.to[immutable.Iterable])
        )
      }
  }
}
