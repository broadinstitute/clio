package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.WgsCramQueryOutput
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockWgsCramService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.WgsCram,
      clioIndex = WgsCramIndex
    ) {

  def emptyOutput: WgsCramQueryOutput = {
    WgsCramQueryOutput(
      Location.GCP,
      "project",
      "sample",
      1
    )
  }
}
