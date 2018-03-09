package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentWgsCram
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput,
  TransferWgsCramV1QueryOutput
}
import org.broadinstitute.clio.util.model.Location

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class MockWgsCramService()(implicit executionContext: ExecutionContext)
    extends MockIndexService[WgsCramIndex.type, DocumentWgsCram](
      transferIndex = WgsCramIndex
    ) {

  val queryCalls: ArrayBuffer[TransferWgsCramV1QueryInput] =
    ArrayBuffer.empty[TransferWgsCramV1QueryInput]

  val queryAllCalls: ArrayBuffer[TransferWgsCramV1QueryInput] =
    ArrayBuffer.empty[TransferWgsCramV1QueryInput]

  val upsertCalls: ArrayBuffer[(TransferWgsCramV1Key, TransferWgsCramV1Metadata)] =
    ArrayBuffer.empty[(TransferWgsCramV1Key, TransferWgsCramV1Metadata)]

  def emptyOutput: TransferWgsCramV1QueryOutput = {
    TransferWgsCramV1QueryOutput(
      Location.GCP,
      "project",
      "sample",
      1
    )
  }
}
