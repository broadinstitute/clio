package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

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
    * @param documents A Seq of (potentially partial) metadata documents containing
    *                  new fields to set on the document in the index.
    * @param index    The index in which to update the document.
    */
  def updateMetadata(documents: Seq[Json])(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit]

  /**
    * Update-or-insert (upsert) metadata into an index.
    *
    * @param document A (potentially partial) metadata document containing
    *                  new fields to set on the document in the index.
    * @param index    The index in which to update the document.
    */
  def updateMetadata(document: Json)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit]

  /**
    * Submit a raw json query of a metadata index
    *
    * @param json  The json string of the query to run.
    * @param index  The index to run the query against.
    */
  def rawQuery(json: String)(
    implicit index: ElasticsearchIndex[_]
  ): Source[Json, NotUsed]

  /**
    * Given an elastic search index, return the most recent document for that index.
    *
    * Document ordering is determined by the "upsert ID", which must be an indexed field.
    *
    * @param index the elasticsearch index
    * @return the most recent document for this index, if any
    */
  def getMostRecentDocument(implicit index: ElasticsearchIndex[_]): Future[Option[Json]]
}
