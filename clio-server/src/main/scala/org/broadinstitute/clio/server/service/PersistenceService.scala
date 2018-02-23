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
    * @param transferKey      The DTO for the key.
    * @param existingTransferMetadata The DTO for the existing metadata.
    * @param newTransferMetadata The DTO for the new metadata.
    * @tparam TK The type of the Transfer Key DTO.
    * @tparam TM The type of the Transfer Metadata DTO.
    * @return the ID for this upsert
    */
  def upsertMetadata[TK <: TransferKey, TM <: TransferMetadata[TM]](
    transferKey: TK,
    existingTransferMetadata: TM,
    newTransferMetadata: TM,
    documentMapper: ElasticsearchDocumentMapper[TK, TM]
  )(implicit ec: ExecutionContext, index: ElasticsearchIndex[TK, TM]): Future[UpsertId] = {
    val (empty, _) = documentMapper.empty(transferKey)
    val (_, overlaidMetadata) = documentMapper.withMetadata(empty, existingTransferMetadata, newTransferMetadata)

    val document = index.encoder.apply((transferKey, overlaidMetadata))

    for {
      _ <- persistenceDAO.writeUpdate(document)
      _ <- searchDAO.updateMetadata(document)
    } yield {
      new UpsertId(document.findAllByKey(UpsertId.UpsertIdFieldName).head.findAllByKey("id"))
    }
  }
}

object PersistenceService {

  def apply(app: ClioApp): PersistenceService = {
    new PersistenceService(app.persistenceDAO, app.searchDAO)
  }
}
