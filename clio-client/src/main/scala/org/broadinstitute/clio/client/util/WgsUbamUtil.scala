package org.broadinstitute.clio.client.util

import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1QueryOutput
}

object WgsUbamUtil {

  implicit class TransferWgsUbamV1KeyUtil(key: TransferWgsUbamV1Key) {
    def prettyKey(): String = {
      s"FlowcellBarcode: ${key.flowcellBarcode}, LibraryName: ${key.libraryName}, " +
        s"Lane: ${key.lane}, Location: ${key.location}"
    }
  }

  implicit class TransferWgsUbamV1QueryOutputUtil(
    output: TransferWgsUbamV1QueryOutput
  ) {
    def prettyKey(): String = {
      TransferWgsUbamV1Key(
        flowcellBarcode = output.flowcellBarcode,
        libraryName = output.libraryName,
        lane = output.lane,
        location = output.location
      ).prettyKey()
    }
  }
}
