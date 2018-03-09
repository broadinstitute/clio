package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentWgsUbam
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata,
  TransferUbamV1QueryInput,
  TransferUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.Location

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class MockWgsUbamService()(implicit executionContext: ExecutionContext)
    extends MockIndexService[WgsUbamIndex.type, DocumentWgsUbam](
      transferIndex = WgsUbamIndex
    ) {

  val queryCalls: ArrayBuffer[TransferUbamV1QueryInput] =
    ArrayBuffer.empty[TransferUbamV1QueryInput]

  val queryAllCalls: ArrayBuffer[TransferUbamV1QueryInput] =
    ArrayBuffer.empty[TransferUbamV1QueryInput]

  val upsertCalls: ArrayBuffer[(TransferUbamV1Key, TransferUbamV1Metadata)] =
    ArrayBuffer.empty[(TransferUbamV1Key, TransferUbamV1Metadata)]

  def emptyOutput: TransferUbamV1QueryOutput = {
    TransferUbamV1QueryOutput(
      "barcode",
      1,
      "library",
      Location.GCP
    )
  }
}
