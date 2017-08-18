package org.broadinstitute.clio.util.json

import org.broadinstitute.clio.transfer.model.TransferReadGroupV1QueryOutput

import io.circe.Json

object JsonSchemas {
  val ReadGroup: Json = JsonSchema[TransferReadGroupV1QueryOutput].toJson
}
