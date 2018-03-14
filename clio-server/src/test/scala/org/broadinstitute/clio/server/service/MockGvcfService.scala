package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1QueryOutput
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockGvcfService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.Gvcf,
      transferIndex = GvcfIndex
    ) {

  def emptyOutput: TransferGvcfV1QueryOutput = {
    TransferGvcfV1QueryOutput(
      Location.GCP,
      "project",
      "sample",
      1
    )
  }
}
