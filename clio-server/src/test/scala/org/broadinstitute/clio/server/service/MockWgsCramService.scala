package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.TransferWgsCramV1QueryOutput
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockWgsCramService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.WgsCram,
      transferIndex = WgsCramIndex
    ) {

  def emptyOutput: TransferWgsCramV1QueryOutput = {
    TransferWgsCramV1QueryOutput(
      Location.GCP,
      "project",
      "sample",
      1
    )
  }
}
