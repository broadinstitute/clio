package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchDocumentMapper,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}

import akka.stream.Materializer
import com.sksamuel.elastic4s.{HitReader, Indexable}
import io.circe.Decoder

import scala.concurrent.{ExecutionContext, Future}

import java.util.UUID

/**
  * Service responsible for persisting metadata updates first by
  * writing them to storage, then by using them to update a search index.
  *
  * Updates are written to storage to serve as the system of record for Clio,
  * allowing for update playback during disaster recovery.
  */
class PersistenceService private (
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO
)(implicit mat: Materializer) {

  /**
    * Update-or-insert (upsert) metadata for a given key.
    *
    * @param transferKey      The DTO for the key.
    * @param transferMetadata The DTO for the metadata.
    * @param index            The index for the document to use in search.
    * @tparam TK The type of the Transfer Key DTO.
    * @tparam TM The type of the Transfer Metadata DTO.
    * @tparam D  The type of the Document.
    * @return A future result of the upsert.
    */
  def upsertMetadata[TK, TM, D <: ClioDocument: Indexable](
    transferKey: TK,
    transferMetadata: TM,
    index: ElasticsearchIndex[D],
    documentMapper: ElasticsearchDocumentMapper[TK, TM, D]
  )(implicit ec: ExecutionContext): Future[UUID] = {
    val empty = documentMapper.empty(transferKey)
    val document = documentMapper.withMetadata(empty, transferMetadata)

    for {
      _ <- persistenceDAO.writeUpdate(document, index)
      _ <- searchDAO.updateMetadata(document, index)
    } yield {
      document.upsertId
    }
  }

  private val countZero = Future.successful(0L)

  /**
    * Recover missing documents for a given Elasticsearch index
    * from the source of truth.
    *
    * Returns the number of updates pulled & applied on success.
    *
    * Fails immediately if an update pulled from the source of
    * truth fails to be applied to the search index.
    */
  def recoverMetadata[D <: ClioDocument: Indexable: HitReader: Decoder](
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext): Future[Long] = {
    searchDAO.getMostRecentDocument(index).flatMap { maybeLastDocument =>
      maybeLastDocument.fold(countZero) { lastDocument =>
        for {
          documents <- persistenceDAO.getAllSince(lastDocument.upsertId, index)
          count <- updateAndCount(documents, index)
        } yield {
          count
        }
      }
    }
  }

  /**
    * Apply a sequence of partial documents to an Elasticsearch index,
    * returning the total number of documents applied.
    *
    * Updates run serially, and short-circuit if applying any one
    * update fails.
    */
  private def updateAndCount[D <: ClioDocument: Indexable](
    documents: Seq[D],
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext): Future[Long] = {
    documents.foldLeft(countZero) {
      case (countFut, document) => {
        for {
          count <- countFut
          _ <- searchDAO.updateMetadata(document, index)
        } yield {
          count + 1
        }
      }
    }
  }
}

object PersistenceService {
  def apply(app: ClioApp)(implicit mat: Materializer): PersistenceService = {
    new PersistenceService(app.persistenceDAO, app.searchDAO)
  }
}
