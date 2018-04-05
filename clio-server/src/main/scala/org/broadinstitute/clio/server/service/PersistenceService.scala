package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchDocumentMapper,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.transfer.model.{IndexKey, Metadata}
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.ExecutionContext

/**
  * Service responsible for persisting metadata updates first by
  * writing them to storage, then by using them to update a search index.
  *
  * Updates are written to storage to serve as the system of record for Clio,
  * allowing for update playback during disaster recovery.
  */
class PersistenceService private[server] (
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO
) {

  /**
    * Update-or-insert (upsert) metadata for a given key.
    *
    * @param key      The DTO for the key
    * @param metadata The DTO for the metadata.
    * @tparam Key      The type of the IndexKey DTO.
    * @tparam M The type of the Metadata DTO.
    * @return the ID for this upsert
    */
  def upsertMetadata[Key <: IndexKey, M <: Metadata[M]](
    key: Key,
    metadata: M,
    documentMapper: ElasticsearchDocumentMapper[Key, M],
    index: ElasticsearchIndex[_]
  )(implicit ec: ExecutionContext): Source[UpsertId, NotUsed] = {
    val document = documentMapper.document(key, metadata)

    Source.fromFuture(for {
      _ <- persistenceDAO.writeUpdate(document, index)
      _ <- searchDAO.updateMetadata(document)(index)
    } yield {
      ElasticsearchIndex.getUpsertId(document)
    })
  }
}
