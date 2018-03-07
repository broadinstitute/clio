package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{DocumentWgsCram, ElasticsearchIndex}
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{TransferWgsCramV1Key, TransferWgsCramV1Metadata, TransferWgsCramV1QueryInput, TransferWgsCramV1QueryOutput}
import org.broadinstitute.clio.util.model.Location

import scala.collection.mutable.ArrayBuffer

class MockWgsCramService() extends MockIndexService[WgsCramIndex.type, DocumentWgsCram](
  transferIndex = WgsCramIndex,
  elasticsearchIndex = ElasticsearchIndex.WgsCram
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