package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.{HitReader, Indexable}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.sksamuel.elastic4s.searches.queries.QueryDefinition

import scala.concurrent.Future

/**
  * Communicates with a search server.
  */
trait SearchDAO {

  /**
    * Checks the status.
    */
  def checkOk: Future[Unit]

  /**
    * Initialize the ready search application.
    */
  def initialize(): Future[Unit]

  /**
    * Closes the connection.
    */
  def close(): Future[Unit]

  /**
    * Update-or-insert (upsert) metadata into an index.
    *
    * @param document A (potentially partial) metadata document containing
    *                 new fields to set on the document in the index.
    * @param index    The index in which to update the document.
    * @tparam D The type of the document.
    */
  def updateMetadata[D <: ClioDocument: Indexable](
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit]

  /**
    * Query a metadata index.
    *
    * @param queryDefinition       The query to run.
    * @param index       The index to run the query against.
    * @tparam D          The type of the document to query.
    */
  def queryMetadata[D <: ClioDocument: HitReader](
    queryDefinition: QueryDefinition,
    index: ElasticsearchIndex[D]
  ): Future[Seq[D]]
}
