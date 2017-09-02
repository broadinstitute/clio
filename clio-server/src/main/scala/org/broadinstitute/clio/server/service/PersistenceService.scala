package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchDocumentMapper,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}

import com.sksamuel.elastic4s.Indexable

import scala.concurrent.{ExecutionContext, Future}

import java.util.UUID

/**
  * Service responsible for persisting metadata updates first by
  * writing them to storage, then by using them to update a search index.
  *
  * Updates are written to storage to serve as the system of record for Clio,
  * allowing for update playback during disaster recovery.
  */
class PersistenceService private (persistenceDAO: PersistenceDAO,
                                  searchDAO: SearchDAO) {

  /**
    * Update-or-insert (upsert) metadata.
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
    val id = documentMapper.id(transferKey)
    val empty = documentMapper.empty(transferKey)
    val document = documentMapper.withMetadata(empty, transferMetadata)

    for {
      _ <- persistenceDAO.writeUpdate(document, index)
      _ <- searchDAO.updateMetadata(id, document, index)
    } yield {
      document.clioId
    }
  }
}

object PersistenceService {
  def apply(app: ClioApp): PersistenceService = {
    new PersistenceService(app.persistenceDAO, app.searchDAO)
  }
}
