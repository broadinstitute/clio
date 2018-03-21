package org.broadinstitute.clio.server.service

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.ArraysKey
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.ExecutionContext

class MockArraysService()(implicit executionContext: ExecutionContext)
    extends MockIndexService(
      elasticsearchIndex = ElasticsearchIndex.Arrays,
      clioIndex = ArraysIndex
    ) {

  val emptyOutput: Json = {
    ArraysKey(
      Location.GCP,
      Symbol("chipwell_barcode"),
      1,
      1
    ).asJson(ArraysIndex.keyEncoder)
  }
}
