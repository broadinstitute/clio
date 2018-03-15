package org.broadinstitute.clio.transfer.model.wgscram

import org.broadinstitute.clio.transfer.model.IndexKey
import org.broadinstitute.clio.util.model.Location

case class WgsCramKey(
  location: Location,
  project: String,
  sampleAlias: String,
  version: Int
) extends IndexKey {

  override def getUrlSegments: Seq[String] =
    Seq(location.entryName, project, sampleAlias, version.toString)
}
