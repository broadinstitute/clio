package org.broadinstitute.clio.transfer.model.wgscram

import org.broadinstitute.clio.transfer.model.IndexKey
import org.broadinstitute.clio.util.model.{DataType, Location}

case class CramKey(
  location: Location,
  project: String,
  sampleAlias: String,
  version: Int,
  dataType: DataType
) extends IndexKey {

  override def getUrlSegments: Seq[String] =
    Seq(location.entryName, project, sampleAlias, version.toString, dataType.entryName)
}
