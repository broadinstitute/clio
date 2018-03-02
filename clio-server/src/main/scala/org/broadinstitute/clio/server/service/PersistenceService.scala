package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{ElasticsearchDocumentMapper, ElasticsearchIndex}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.transfer.model.{TransferKey, TransferMetadata}
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
    * @param transferKey      The DTO for the key
    * @param transferMetadata The DTO for the metadata.
    * @tparam Key      The type of the TransferKey DTO.
    * @tparam Metadata The type of the TransferMetadata DTO.
    * @return the ID for this upsert
    */
  def upsertMetadata[Key <: TransferKey, Metadata <: TransferMetadata[Metadata]](
    transferKey: Key,
    transferMetadata: Metadata,
    documentMapper: ElasticsearchDocumentMapper[Key, Metadata]
  )(implicit ec: ExecutionContext, index: ElasticsearchIndex[_]): Future[UpsertId] = {
    val document = documentMapper.document(transferKey, transferMetadata)

    for {
      _ <- persistenceDAO.writeUpdate(document)
      _ <- searchDAO.updateMetadata(document)
    } yield {
      ElasticsearchIndex.getUpsertId(document)
    }
  }
}

object PersistenceService {

  def apply(app: ClioApp): PersistenceService = {
    new PersistenceService(app.persistenceDAO, app.searchDAO)
  }
}
