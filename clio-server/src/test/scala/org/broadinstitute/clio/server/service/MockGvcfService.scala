package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.GvcfQueryOutput
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockGvcfService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.Gvcf,
      clioIndex = GvcfIndex
    ) {

  def emptyOutput: GvcfQueryOutput = {
    GvcfQueryOutput(
      Location.GCP,
      "project",
      "sample",
      1
    )
  }
}
