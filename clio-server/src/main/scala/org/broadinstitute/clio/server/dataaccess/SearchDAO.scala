package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.collection.immutable
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
  def initialize(indexes: immutable.Seq[ElasticsearchIndex[_]]): Future[Unit]

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
  def updateMetadata[D <: ClioDocument](document: D)(
    implicit index: ElasticsearchIndex[D]
  ): Future[Unit]

  /**
    * Query a metadata index.
    *
    * @param queryDefinition       The query to run.
    * @param index       The index to run the query against.
    * @tparam D          The type of the document to query.
    */
  def queryMetadata[D <: ClioDocument](queryDefinition: QueryDefinition)(
    implicit index: ElasticsearchIndex[D]
  ): Source[D, NotUsed]

  /**
    * Given an elastic search index, return the most recent document for that index.
    *
    * Document ordering is determined by the "upsert ID", which must be an indexed field.
    *
    * @param index the elasticsearch index
    * @tparam D the document type this index contains
    * @return the most recent document for this index, if any
    */
  def getMostRecentDocument[D <: ClioDocument](
    implicit index: ElasticsearchIndex[D]
  ): Future[Option[D]]
}
