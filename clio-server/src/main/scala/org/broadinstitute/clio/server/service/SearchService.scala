package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchDocumentMapper,
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}

import com.sksamuel.elastic4s.{HitReader, Indexable}

import scala.concurrent.{ExecutionContext, Future}

import java.util.UUID

class SearchService private (searchDAO: SearchDAO) {

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
      _ <- searchDAO.updateMetadata(id, document, index)
    } yield {
      document.clioId
    }
  }

  /**
    * Run a query.
    *
    * @param transferInput The DTO for the query input.
    * @param index         The index to search.
    * @param queryMapper   Converts the DTO into a search query.
    * @tparam TI The type of the Transfer Query Input DTO.
    * @tparam TO The type of the Transfer Query Output DTO.
    * @tparam D  The type of the Document.
    * @return The result of the query.
    */
  def queryMetadata[TI, TO, D: HitReader](
    transferInput: TI,
    index: ElasticsearchIndex[D],
    queryMapper: ElasticsearchQueryMapper[TI, TO, D]
  ): Future[Seq[TO]] = {
    searchDAO.queryMetadata(transferInput, index, queryMapper)
  }
}

object SearchService {
  def apply(app: ClioApp): SearchService = {
    new SearchService(app.searchDAO)
  }
}
