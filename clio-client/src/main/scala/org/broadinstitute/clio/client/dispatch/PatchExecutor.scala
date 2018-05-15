package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import better.files.File
import cats.syntax.either._
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.PatchCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.JsonUtils.JsonOps

class PatchExecutor[CI <: ClioIndex](patchCommand: PatchCommand[CI]) extends Executor {

  import patchCommand.index.implicits._
  import Executor.SourceMonadOps

  private type DocumentKey = patchCommand.index.KeyType

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
    ioUtil
      .readMetadata(patchCommand.index)(patchCommand.metadataLocation)
      .map(_.asJson.dropNulls)
      .flatMap(Source.repeat)
      // zip new metadatas with documents
      .zip(
        webClient
          .jsonFileQuery(patchCommand.index)(queryAllFile)
          .map(_.dropNulls)
      )
      // Munge metadata for documents
      .map {
        case (newMetadataJson, documentJson) =>
          mungeMetadata(newMetadataJson, documentJson)
      }
      // filter out things that already have no new metadata to upsert
      .filterNot {
        case (mergedJson, oldMetadataJson, _) =>
          mergedJson.equals(oldMetadataJson)
      }
      // Upsert the stuff!
      .flatMapConcat {
        case (mergedMetadataJson, oldMetadataJson, docKey) =>
          upsertPatch(mergedMetadataJson, oldMetadataJson, docKey, webClient)
      }
  }

  /**
    * Munge the patch metadata with the document metadata to get a new metadata json
    * @param patchMetadataJson The new metadata to insert into the index
    * @param documentJson The old document to insert the metadata into
    * @return A tuple containing the munged metadata, the old metadata, and the key for this document
    */
  private def mungeMetadata(
    patchMetadataJson: Json,
    documentJson: Json
  ): (Json, Json, DocumentKey) = {
    val oldMetadataJson = getMetadataJsonFromDocument(documentJson)
    val indexKey = getKeyFromDocument(documentJson)
    (
      patchMetadataJson.deepMerge(oldMetadataJson),
      oldMetadataJson,
      indexKey
    )
  }

  /**
    * Upsert the new metadata into the document.
    * This method strips any existing keys out of the metadata to be upserted in order to reduce traffic.
    * @param mergedMetadataJson The merged metadata containing patch and existing metadata
    * @param oldMetadataJson The old, preexisting metadata
    * @param docKey The key for this metadata
    * @param webClient A ClioWebClient to upsert with.
    * @return
    */
  private def upsertPatch(
    mergedMetadataJson: Json,
    oldMetadataJson: Json,
    docKey: DocumentKey,
    webClient: ClioWebClient
  ): Source[Json, NotUsed] = {
    val oldMetadataKeys =
      oldMetadataJson.asObject.map(_.keys.toSeq).getOrElse(Seq.empty)
    val patchMetadata = mergedMetadataJson
      .mapObject(_.filterKeys(key => !oldMetadataKeys.contains(key)))
      .as[patchCommand.index.MetadataType]
      .valueOr(
        ex =>
          throw new IllegalArgumentException(
            "Could not encode new metadata json into class",
            ex
        )
      )
    webClient.upsert(patchCommand.index)(docKey, patchMetadata, force = false)
  }

  /**
    * Get a IndexKey from a json document returned from Elasticsearch
    * @param json document of both key and metadata fields
    * @return An IndexKey that identifies the document
    */
  private def getKeyFromDocument(json: Json): DocumentKey = {
    val keyJson =
      json.mapObject(
        _.filterKeys(Executor.getJsonKeyFieldNames(patchCommand.index).contains)
      )

    keyJson
      .as[DocumentKey]
      .valueOr(
        ex =>
          throw new IllegalArgumentException(
            s"Could not create ${patchCommand.index.keyTag} from ${json.pretty(implicitly)}",
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
        key => !Executor.getJsonKeyFieldNames(patchCommand.index).contains(key)
      )
    )
  }

}
