package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model._

import scala.concurrent.Future

class MemoryWgsUbamSearchDAO extends MockSearchDAO {

  var updateWgsUbamMetadataCalls: Seq[(ModelWgsUbamKey, ModelWgsUbamMetadata)] =
    Seq.empty
  var queryWgsUbamCalls: Seq[ModelWgsUbamQueryInput] = Seq.empty

  override def updateWgsUbamMetadata(
      key: ModelWgsUbamKey,
      metadata: ModelWgsUbamMetadata
  ): Future[Unit] = {
    updateWgsUbamMetadataCalls :+= ((key, metadata))
    super.updateWgsUbamMetadata(key, metadata)
  }

  override def queryWgsUbam(
      queryInput: ModelWgsUbamQueryInput
  ): Future[Seq[ModelWgsUbamQueryOutput]] = {
    queryWgsUbamCalls :+= queryInput
    super.queryWgsUbam(queryInput)
  }
}
