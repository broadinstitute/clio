package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchDocumentMapper,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.{ExecutionContext, Future}

/**
  * Service responsible for persisting metadata updates first by
  * writing them to storage, then by using them to update a search index.
  *
  * Updates are written to storage to serve as the system of record for Clio,
  * allowing for update playback during disaster recovery.
  */
class PersistenceService private (persistenceDAO: PersistenceDAO, searchDAO: SearchDAO) {

  /**
    * Update-or-insert (upsert) metadata for a given key.
    *
    * @param transferKey      The DTO for the key.
    * @param transferMetadata The DTO for the metadata.
    * @tparam TK The type of the Transfer Key DTO.
    * @tparam TM The type of the Transfer Metadata DTO.
    * @tparam D  The type of the Document.
    * @return the ID for this upsert
    */
  def upsertMetadata[TK, TM, D <: ClioDocument: ElasticsearchIndex](
    transferKey: TK,
    transferMetadata: TM,
    documentMapper: ElasticsearchDocumentMapper[TK, TM, D]
  )(
    implicit
    ec: ExecutionContext
  ): Future[UpsertId] = {
    val empty = documentMapper.empty(transferKey)
    val document = documentMapper.withMetadata(empty, transferMetadata)

    for {
      _ <- persistenceDAO.writeUpdate(document)
      _ <- searchDAO.updateMetadata(document)
    } yield {
      document.upsertId
    }
  }
}

object PersistenceService {

  def apply(persistenceDAO: PersistenceDAO, searchDAO: SearchDAO): PersistenceService = {
    new PersistenceService(persistenceDAO, searchDAO)
  }
}
