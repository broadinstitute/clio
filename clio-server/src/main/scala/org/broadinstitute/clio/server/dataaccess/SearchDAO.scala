package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model._

import scala.concurrent.Future

import java.util.UUID

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
    * Updates the wgs ubam metadata.
    *
    * @param key      The key to the wgs ubam.
    * @param metadata The new fields for the metadata.
    * @return The result of the update.
    */
  def updateWgsUbamMetadata(key: ModelWgsUbamKey,
                            metadata: ModelWgsUbamMetadata): Future[UUID]

  /**
    * Query the wgs ubams.
    *
    * @param queryInput The query input.
    * @return The query outputs.
    */
  def queryWgsUbam(
    queryInput: ModelWgsUbamQueryInput
  ): Future[Seq[ModelWgsUbamQueryOutput]]
}
