package org.broadinstitute.clio.server.service

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.GvcfKey
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockGvcfService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.Gvcf,
      clioIndex = GvcfIndex
    ) {

  val emptyOutput: Json =
    GvcfKey(
      Location.GCP,
      "project",
      "sample",
      1
    ).asJson(GvcfIndex.objectKeyEncoder)
}
