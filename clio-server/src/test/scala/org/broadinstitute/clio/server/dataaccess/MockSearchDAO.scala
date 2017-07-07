package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model._

import scala.concurrent.Future

class MockSearchDAO extends SearchDAO {
  override def checkOk: Future[Unit] = {
    Future.successful(())
  }

  override def initialize(): Future[Unit] = {
    Future.successful(())
  }

  override def close(): Future[Unit] = {
    Future.successful(())
  }

  override def updateReadGroupMetadata(
    key: ModelReadGroupKey,
    metadata: ModelReadGroupMetadata
  ): Future[Unit] = {
    Future.successful(())
  }

  override def queryReadGroup(
    queryInput: ModelReadGroupQueryInput
  ): Future[Seq[ModelReadGroupQueryOutput]] = {
    Future.successful(Seq())
  }
}
