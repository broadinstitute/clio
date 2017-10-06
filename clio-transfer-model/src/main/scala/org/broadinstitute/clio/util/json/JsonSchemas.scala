package org.broadinstitute.clio.util.json

import io.circe.Json
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1QueryOutput
import org.broadinstitute.clio.transfer.model.wgscram.TransferWgsCramV1QueryOutput
import org.broadinstitute.clio.transfer.model.wgsubam.TransferWgsUbamV1QueryOutput

object JsonSchemas {
  val WgsUbam: Json = JsonSchema[TransferWgsUbamV1QueryOutput].toJson
  val Gvcf: Json = JsonSchema[TransferGvcfV1QueryOutput].toJson
  val WgsCram: Json = JsonSchema[TransferWgsCramV1QueryOutput].toJson
}
