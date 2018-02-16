package org.broadinstitute.clio.client.dispatch

import io.circe.parser.parse
import org.broadinstitute.clio.client.commands.{AddCommand, ClioCommand}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.generic.{CaseClassMapper, CaseClassTypeConverter}
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
      new RuntimeException(
        s"Could not parse contents of $location as JSON.",
        err
      )
    }

    val decodedOrError = parsedOrError
      .flatMap(_.as[addCommand.index.MetadataType])
      .left
      .map { err =>
        new RuntimeException(
          s"Invalid metadata given at $location. Run the '${ClioCommand.getSchemaPrefix}$commandName' command to see the expected JSON format for ${name}s.",
          err
        )
      }

    decodedOrError.fold(
      Future
        .failed(_) logErrorMsg s"Metadata at $location cannot be added to Clio", {
        decoded =>
          {
            for {
              existingMetadata <- if (!addCommand.forceUpdate) {
                queryForKey(webClient).logErrorMsg(
                  s"Could not query the $name. No files have been added."
                )
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
          .logErrorMsg(
            s"An error occurred while adding the $name record in Clio."
          ),
        new RuntimeException(
          "Adding this document will overwrite the following existing metadata: " +
            differences
              .map(
                diff => s"Field: ${diff._1} Old value: ${diff._3} New value: ${diff._2}"
              )
              .mkString("\n") +
            " Use '--force-update' to overwrite this data."
        )
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
      mapper.vals(existingMetadata).filterNot(_._2 == None)

    val newMetadataValues =
      mapper.vals(newMetadata).filterNot(_._2 == None)

    val differentFields =
      newMetadataValues.keySet
        .intersect(existingMetadataValues.keySet)
        .filterNot(
          field => existingMetadataValues.get(field).equals(newMetadataValues.get(field))
        )

    for (key <- differentFields)
      yield
        (
          key,
          newMetadataValues.getOrElse(key, None),
          existingMetadataValues.getOrElse(key, None)
        )
  }

  private def queryForKey(
    client: ClioWebClient
  )(implicit ec: ExecutionContext): Future[Option[addCommand.index.MetadataType]] = {

    val keyToQueryMapper = CaseClassTypeConverter[
      addCommand.index.KeyType,
      addCommand.index.QueryInputType
    ](vals => vals.mapValues((v: Any) => Option(v)))

    val keyFields = new CaseClassMapper[addCommand.index.KeyType].names
    val outputToMetadataMapper = CaseClassTypeConverter[
      addCommand.index.QueryOutputType,
      addCommand.index.MetadataType
    ](inKeys => inKeys -- keyFields)

    val queryResponse = client
      .query(addCommand.index)(
        keyToQueryMapper.convert(addCommand.key),
        includeDeleted = false
      )
      .logErrorMsg("There was an error contacting the Clio server.")

    val queryOutputs = queryResponse
      .map(
        _.as[Seq[addCommand.index.QueryOutputType]]
          .fold(throw _, identity)
      )

    queryOutputs.map { outputs =>
      val commandName = addCommand.index.commandName

      outputs.toList match {
        case Nil           => None
        case output :: Nil => Some(outputToMetadataMapper.convert(output))
        case _ =>
          throw new Exception(
            s"${outputs.length} ${name}s were returned for $prettyKey, expected 1. Use ${ClioCommand.queryPrefix}$commandName to see what was returned."
          )
      }
    }
  }
}
