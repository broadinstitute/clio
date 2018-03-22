package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.ArraysQueryOutput
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockArraysService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.Arrays,
      clioIndex = ArraysIndex
    ) {

  def emptyOutput: ArraysQueryOutput = {
    ArraysQueryOutput(
      Location.GCP,
      Symbol("chipwell_barcode"),
      1,
      1
    )
  }
}
