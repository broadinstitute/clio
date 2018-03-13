package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.TransferUbamV1QueryOutput
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockWgsUbamService()(implicit executionContext: ExecutionContext)
    extends MockIndexService[WgsUbamIndex.type](
      elasticsearchIndex = ElasticsearchIndex.WgsUbam,
      transferIndex = WgsUbamIndex
    ) {

  def emptyOutput: TransferUbamV1QueryOutput = {
    TransferUbamV1QueryOutput(
      "barcode",
      1,
      "library",
      Location.GCP
    )
  }
}
