package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model._

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
    * Updates the read group metadata.
    *
    * @param key      The key to the read group.
    * @param metadata The new fields for the metadata.
    * @return The result of the update.
    */
  def updateReadGroupMetadata(key: ModelReadGroupKey,
                              metadata: ModelReadGroupMetadata): Future[Unit]

  /**
    * Query the read groups.
    *
    * @param queryInput The query input.
    * @return The query outputs.
    */
  def queryReadGroup(
    queryInput: ModelReadGroupQueryInput
  ): Future[Seq[ModelReadGroupQueryOutput]]
}
