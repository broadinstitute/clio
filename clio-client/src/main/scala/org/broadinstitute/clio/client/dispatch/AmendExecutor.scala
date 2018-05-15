package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import better.files.File
import cats.syntax.either._
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.AmendCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.JsonUtils.JsonOps

class AmendExecutor[CI <: ClioIndex](amendCommand: AmendCommand[CI]) extends Executor {

  import amendCommand.index.implicits._
  import Executor.SourceMonadOps

  private val queryAllFile: File =
    File.newTemporaryFile().deleteOnExit().write(Json.obj().pretty(implicitly))

  /**
    * Build a stream which, when pulled, will communicate with the clio-server
    * to update its records about some metadata, potentially performing IO
    * operations in the process.
    */
  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    // Read the metadata then turn into Json so that we fail fast.
    val newMetadataSource =
      Executor
        .readMetadata(amendCommand.index)(amendCommand.metadataLocation, ioUtil)
        .map(_.asJson.dropNulls)
        .flatMap(Source.repeat)

    // zip new metadatas with documents
    newMetadataSource
      .zip(
        webClient
          .jsonFileQuery(amendCommand.index)(queryAllFile)
          .map(_.dropNulls)
      )
      // Munge metadata for documents
      .map {
        case (newMetadataJson, documentJson) =>
          val oldMetadataJson = getMetadataJsonFromDocument(documentJson)
          val indexKey = getKeyFromDocument(documentJson)
          (
            newMetadataJson.deepMerge(getMetadataJsonFromDocument(documentJson)),
            oldMetadataJson,
            indexKey
          )
      }
      // filter out things that already have no new metadata to upsert
      .filterNot {
        case (mergedJson, oldMetadataJson, _) =>
          mergedJson.equals(oldMetadataJson)
      }
      // Upsert the stuff!
      .flatMapConcat {
        case (mergedMetadataJson, oldMetadataJson, indexKey) =>
          val oldMetadataKeys =
            oldMetadataJson.asObject.map(_.keys.toSeq).getOrElse(Seq.empty)
          val metadata = mergedMetadataJson
            .mapObject(_.filterKeys(key => !oldMetadataKeys.contains(key)))
            .as[amendCommand.index.MetadataType]
            .valueOr(
              ex =>
                throw new IllegalArgumentException(
                  "Could not encode new metadata json into class",
                  ex
              )
            )
          webClient.upsert(amendCommand.index)(indexKey, metadata, force = false)

      }
  }

  /**
    * Get a IndexKey from a json document returned from Elasticsearch
    * @param json document of both key and metadata fields
    * @return An IndexKey that identifies the document
    */
  private def getKeyFromDocument(json: Json): amendCommand.index.KeyType = {
    val keyJson =
      json.mapObject(
        _.filterKeys(Executor.getJsonKeyFieldNames(amendCommand.index).contains)
      )

    keyJson
      .as[amendCommand.index.KeyType]
      .valueOr(
        ex =>
          throw new IllegalArgumentException(
            s"Could not create ${amendCommand.index.keyTag} from ${json.pretty(implicitly)}",
            ex
        )
      )
  }

  /**
    * Get the metadata fields only from a json document returned from Elasticseach
    * @param json document of both key and metadata fields
    * @return a json with only the metadata fields
    */
  private def getMetadataJsonFromDocument(json: Json): Json = {
    json.mapObject(
      _.filterKeys(
        key => !Executor.getJsonKeyFieldNames(amendCommand.index).contains(key)
      )
    )
  }

}
