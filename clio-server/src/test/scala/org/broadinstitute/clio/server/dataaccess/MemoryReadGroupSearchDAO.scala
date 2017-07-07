package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model._

import scala.concurrent.Future

class MemoryReadGroupSearchDAO extends MockSearchDAO {

  var updateReadGroupMetadataCalls
    : Seq[(ModelReadGroupKey, ModelReadGroupMetadata)] = Seq.empty
  var queryReadGroupCalls: Seq[ModelReadGroupQueryInput] = Seq.empty

  override def updateReadGroupMetadata(
    key: ModelReadGroupKey,
    metadata: ModelReadGroupMetadata
  ): Future[Unit] = {
    updateReadGroupMetadataCalls :+= ((key, metadata))
    super.updateReadGroupMetadata(key, metadata)
  }

  override def queryReadGroup(
    queryInput: ModelReadGroupQueryInput
  ): Future[Seq[ModelReadGroupQueryOutput]] = {
    queryReadGroupCalls :+= queryInput
    super.queryReadGroup(queryInput)
  }
}
