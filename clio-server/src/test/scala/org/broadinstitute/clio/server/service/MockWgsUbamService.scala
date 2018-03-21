package org.broadinstitute.clio.server.service

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.UbamKey
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockWgsUbamService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.WgsUbam,
      clioIndex = WgsUbamIndex
    ) {

  val emptyOutput: Json = {
    UbamKey(
      Location.GCP,
      "barcode",
      1,
      "library"
    ).asJson(WgsUbamIndex.keyEncoder)
  }
}
