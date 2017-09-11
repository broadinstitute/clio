package org.broadinstitute.clio.client.util

import org.broadinstitute.clio.transfer.model.{
  TransferGvcfV1Key,
  TransferGvcfV1QueryOutput
}

object GvcfUtil {

  implicit class TransferGvcfV1KeyUtil(key: TransferGvcfV1Key) {
    def prettyKey(): String = {
      s"Project: ${key.project}, SampleAlias: ${key.sampleAlias}, " +
        s"Version: ${key.version}, Location: ${key.location}"
    }
  }

  implicit class TransferGvcfV1QueryOutputUtil(
    output: TransferGvcfV1QueryOutput
  ) {
    def prettyKey(): String = {
      TransferGvcfV1Key(
        location = output.location,
        project = output.project,
        sampleAlias = output.sampleAlias,
        version = output.version
      ).prettyKey()
    }
  }
}
