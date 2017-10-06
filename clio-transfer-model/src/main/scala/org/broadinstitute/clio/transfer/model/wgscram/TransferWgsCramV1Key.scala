package org.broadinstitute.clio.transfer.model.wgscram

import org.broadinstitute.clio.transfer.model.TransferKey
import org.broadinstitute.clio.util.model.Location

case class TransferWgsCramV1Key(location: Location,
                                project: String,
                                sampleAlias: String,
                                version: Int)
  extends TransferKey {

  def getUrlPath: String =
    s"$location/$project/$sampleAlias/$version"
}

object TransferWgsCramV1Key {
  def apply(wgsCram: TransferWgsCramV1QueryOutput): TransferWgsCramV1Key =
    TransferWgsCramV1Key(
      wgsCram.location,
      wgsCram.project,
      wgsCram.sampleAlias,
      wgsCram.version
    )
}
