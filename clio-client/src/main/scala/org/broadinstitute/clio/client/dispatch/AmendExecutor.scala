package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.{DecodingFailure, Json}
import io.circe.syntax._
import org.broadinstitute.clio.client.commands.AmendCommand
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.JsonUtils.JsonOps

class AmendExecutor[CI <: ClioIndex](amendCommand: AmendCommand[CI]) extends Executor {

  import amendCommand.index.implicits._
  import Executor.SourceMonadOps
  import Executor.readMetadata

  private val queryIndexInput: amendCommand.index.QueryInputType = amendCommand.queryInput

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
      readMetadata(amendCommand.index)(amendCommand.metadataLocation, ioUtil)
        .map(_.asJson.dropNulls)
        .flatMap(Source.repeat)

    // zip new metadatas with documents
    newMetadataSource
      .zip(
        webClient
          .simpleQuery(amendCommand.index)(queryIndexInput, includeDeleted = false)
          .map(_.dropNulls)
      )
      // Munge metadata for documents that have partial data
      .map { tup =>
        val (newMetadataJson, documentJson) = tup
        val existingKeys =
          documentJson.asObject.map(_.keys.toSeq).getOrElse(Seq.empty)
        val newMetadataMinusExisting =
          newMetadataJson.mapObject(_.filterKeys(key => !existingKeys.contains(key)))
        (newMetadataMinusExisting, documentJson)
      }
      // filter out things that already have no new metadata to upsert
      .filter { tup =>
        val (newMetadataJson, _) = tup
        newMetadataJson.asObject.exists(obj => !obj.isEmpty)
      }
      // Upsert the stuff!
      .flatMapConcat { tup =>
        val (newMetadataJson, documentJson) = tup
        val key = getKeyFromDocument(documentJson)
        val metadata = newMetadataJson
          .as[amendCommand.index.MetadataType]
          .fold(
            ex =>
              throw new RuntimeException(
                "Could not encode new metadata json into class",
                ex.getCause
            ),
            identity
          )
        webClient.upsert(amendCommand.index)(key, metadata, force = false)

      }
  }

  private def getKeyFromDocument(json: Json): amendCommand.index.KeyType = {
    val keyJson =
      json.mapObject(_.filterKeys(amendCommand.index.keyFieldNames.contains))

    val key: amendCommand.index.KeyType = keyJson.as[amendCommand.index.KeyType] match {
      case Left(ex: DecodingFailure) =>
        throw new RuntimeException(
          s"Could not create ${amendCommand.index.keyTag} from ${json.pretty(implicitly)}",
          ex.getCause
        )
      case Right(k) => k
    }
    key
  }
}
