package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.UbamQueryOutput
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockWgsUbamService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.WgsUbam,
      clioIndex = WgsUbamIndex
    ) {

  def emptyOutput: UbamQueryOutput = {
    UbamQueryOutput(
      "barcode",
      1,
      "library",
      Location.GCP
    )
  }
}
