package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
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

  /**
    * Build a stream which, when pulled, will communicate with the clio-server
    * to update its records about some metadata, potentially performing IO
    * operations in the process.
    */
  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {

    val docsToUpsert = for {
      // Read the metadata then turn into Json so that we fail fast.
      newMetadataJson <- ioUtil
        .readMetadata(patchCommand.index)(patchCommand.metadataLocation)
        .map(_.asJson.dropNulls)

      /*
       * Build a query that will pull all documents with any of the
       * fields-to-patch not set.
       */
      queryJson = PatchExecutor.buildQueryForUnpatched(newMetadataJson)
      documentJson <- webClient
        .query(patchCommand.index)(queryJson, raw = true)
        .map(_.dropNulls)

      (mergedJson, oldMetadataJson, docKey) = mergeMetadata(newMetadataJson, documentJson)
      if !mergedJson.equals(oldMetadataJson)
    } yield {
      (mergedJson, oldMetadataJson, docKey)
    }

    // Upsert the stuff!
    docsToUpsert
      .flatMapMerge(
        patchCommand.parallelism, {
          case (mergedMetadataJson, oldMetadataJson, docKey) =>
            upsertPatch(mergedMetadataJson, oldMetadataJson, docKey, webClient)
        }
      )
      .fold(0) { (count, _) =>
        count + 1
      }
      .map { count =>
        logger.info(s"Patched $count documents.")
        count.asJson
      }
  }

  /**
    * Munge the patch metadata with the document metadata to get a new metadata json
    * @param patchMetadataJson The new metadata to insert into the index
    * @param documentJson The old document to insert the metadata into
    * @return A tuple containing the munged metadata, the old metadata, and the key for this document
    */
  private def mergeMetadata(
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
    * This method strips any existing fields out of the metadata to be upserted in order to reduce network traffic.
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
    logger.debug(s"Upserting patch metadata for document with key: $docKey")
    webClient.upsertJson(patchCommand.index)(docKey, patchMetadata, force = false)
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
            s"Could not create ${patchCommand.index.keyTag} from ${json.pretty(defaultPrinter)}",
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

object PatchExecutor {

  /**
    * Given a JSON object containing metadata fields which should be "patched" into
    * Clio documents, build a JSON object containing an Elasticsearch query which
    * will return only documents with at least one unset value amongst the fields-to-patch.
    *
    * Used to avoid pulling documents from Clio that are guaranteed to not be re-
    * upserted, since patching doesn't overwrite existing values.
    */
  private[dispatch] def buildQueryForUnpatched(patchJson: Json): Json = {
    import io.circe.literal._

    val mustNots = patchJson.withObject { obj =>
      Json.fromValues {
        obj.keys.map { key =>
          json"""{ "bool": { "must_not": { "exists": { "field": $key } } } }"""
        }
      }
    }

    json"""{ "query": { "bool": { "should": $mustNots, "minimum_should_match": 1 } } }"""
  }
}
