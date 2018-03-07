package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{DocumentGvcf, ElasticsearchIndex}
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.{TransferGvcfV1Key, TransferGvcfV1Metadata, TransferGvcfV1QueryInput, TransferGvcfV1QueryOutput}
import org.broadinstitute.clio.util.model.Location

import scala.collection.mutable.ArrayBuffer

class MockGvcfService() extends MockIndexService[GvcfIndex.type, DocumentGvcf](
  transferIndex = GvcfIndex,
  elasticsearchIndex = ElasticsearchIndex.Gvcf
) {
  val queryCalls: ArrayBuffer[TransferGvcfV1QueryInput] =
    ArrayBuffer.empty[TransferGvcfV1QueryInput]
  val queryAllCalls: ArrayBuffer[TransferGvcfV1QueryInput] =
    ArrayBuffer.empty[TransferGvcfV1QueryInput]
  val upsertCalls: ArrayBuffer[(TransferGvcfV1Key, TransferGvcfV1Metadata)] =
    ArrayBuffer.empty[(TransferGvcfV1Key, TransferGvcfV1Metadata)]


  def emptyOutput: TransferGvcfV1QueryOutput = {
    TransferGvcfV1QueryOutput(
      Location.GCP,
      "project",
      "sample",
      1
    )
  }
}
