package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.syntax.either._
import cats.syntax.show._
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
      toUpsert <- mergeMetadata(newMetadataJson, documentJson)
        .fold(Source.empty[(DocumentKey, Json)])(Source.single)
    } yield {
      toUpsert
    }

    // Upsert the stuff!
    docsToUpsert
      .flatMapMerge(
        patchCommand.maxParallelUpserts, {
          case (docKey, patchMetadata) =>
            logger.debug(
              s"Upserting patch metadata for document with key: ${docKey.show}"
            )
            webClient.upsertJson(patchCommand.index)(docKey, patchMetadata, force = false)
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
    * Merge the patch metadata with the document metadata to get a new metadata json.
    *
    * Filter out any fields unchanged by the merge to avoid upserting redundant data,
    * and return nothing if all fields were already set.
    *
    * @param patchMetadataJson The new metadata to insert into the index
    * @param documentJson The old document to insert the metadata into
    * @return An optional pair of key to new-metadata-to-upsert for that key
    */
  private def mergeMetadata(
    patchMetadataJson: Json,
    documentJson: Json
  ): Option[(DocumentKey, Json)] = {
    val oldMetadataJson = getMetadataJsonFromDocument(documentJson)
    val indexKey = getKeyFromDocument(documentJson)

    // deepMerge keeps values from the RHS when there's a conflict.
    val patchedJson = patchMetadataJson.deepMerge(oldMetadataJson)
    Some(patchedJson).filterNot(_.equals(oldMetadataJson)).map { patched =>
      val oldKeys = oldMetadataJson.asObject.fold(Seq.empty[String])(_.keys.toSeq)
      indexKey -> patched.mapObject(_.filterKeys(!oldKeys.contains(_)))
    }
  }

  /**
    * Get a IndexKey from a json document returned from Elasticsearch
    *
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
            s"Could not extract a ${patchCommand.index.name} key from ${json.printWith(defaultPrinter)}",
            ex
        )
      )
  }

  /**
    * Get the metadata fields only from a json document returned from Elasticseach
    *
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
