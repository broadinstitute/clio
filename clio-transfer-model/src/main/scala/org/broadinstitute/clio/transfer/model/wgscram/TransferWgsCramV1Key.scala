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
