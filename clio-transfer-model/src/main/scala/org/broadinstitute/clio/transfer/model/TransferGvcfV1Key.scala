package org.broadinstitute.clio.transfer.model

import org.broadinstitute.clio.util.model.Location

case class TransferGvcfV1Key(location: Location,
                             project: String,
                             sampleAlias: String,
                             version: Int)