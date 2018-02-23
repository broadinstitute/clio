package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import io.circe.Json
import io.circe.syntax._
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
  def initialize(indexes: immutable.Seq[ElasticsearchIndex[_, _]]): Future[Unit]

  /**
    * Closes the connection.
    */
  def close(): Future[Unit]

  def updateMetadata(documents: Json*)(
    implicit index: ElasticsearchIndex[_, _]
  ): Future[Unit]

  /**
    * Query a metadata index.
    *
    * @param queryDefinition       The query to run.
    * @param index       The index to run the query against.
    */
  def queryMetadata(queryDefinition: QueryDefinition)(
    implicit index: ElasticsearchIndex[_, _]
  ): Source[Json, NotUsed]

  /**
    * Given an elastic search index, return the most recent document for that index.
    *
    * Document ordering is determined by the "upsert ID", which must be an indexed field.
    *
    * @param index the elasticsearch index
    * @return the most recent document for this index, if any
    */
  def getMostRecentDocument(implicit index: ElasticsearchIndex[_, _]): Future[Option[Json]]
}
