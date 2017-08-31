package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}

import com.sksamuel.elastic4s.{HitReader, Indexable}

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
    * @param id       The key identifier for the document to upsert.
    * @param document A (potentially partial) metadata document containing
    *                 new fields to set on the document in the index.
    * @param index    The index in which to update the document.
    * @tparam D       The type of the document.
    */
  def updateMetadata[D: Indexable](id: String,
                                   document: D,
                                   index: ElasticsearchIndex[D]): Future[Unit]

  /**
    * Query a metadata index.
    *
    * @param input       An object describing the query to run.
    * @param index       The index to run the query against.
    * @param queryMapper Utility for mapping the query input to an
    *                    elastic4s query object, and for mapping the
    *                    query results back to a query output type.
    * @tparam I          The type of the query input.
    * @tparam O          The type of the query output.
    * @tparam D          The type of the document to query.
    */
  def queryMetadata[I, O, D: HitReader](
    input: I,
    index: ElasticsearchIndex[D],
    queryMapper: ElasticsearchQueryMapper[I, O, D]
  ): Future[Seq[O]]
}
