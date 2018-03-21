package org.broadinstitute.clio.server.service

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.WgsCramKey
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockWgsCramService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.WgsCram,
      clioIndex = WgsCramIndex
    ) {

  val emptyOutput: Json = {
    WgsCramKey(
      Location.GCP,
      "project",
      "sample",
      1
    ).asJson(WgsCramIndex.keyEncoder)
  }
}
