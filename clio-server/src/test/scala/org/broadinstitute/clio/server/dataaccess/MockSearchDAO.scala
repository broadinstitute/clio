package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.util.ClioUUIDGenerator
import org.broadinstitute.clio.server.model._

import scala.concurrent.Future

import java.util.UUID

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

  override def updateWgsUbamMetadata(
    key: ModelWgsUbamKey,
    metadata: ModelWgsUbamMetadata
  ): Future[UUID] = {
    Future.successful(ClioUUIDGenerator.getUUID())
  }

  override def queryWgsUbam(
    queryInput: ModelWgsUbamQueryInput
  ): Future[Seq[ModelWgsUbamQueryOutput]] = {
    Future.successful(Seq())
  }
}
