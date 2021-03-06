package org.broadinstitute.clio.transfer.model.cram

import org.broadinstitute.clio.transfer.model.IndexKey
import org.broadinstitute.clio.util.model.{DataType, Location}

case class CramKey(
  location: Location,
  project: String,
  dataType: DataType,
  sampleAlias: String,
  version: Int
) extends IndexKey {

  override def getUrlSegments: Seq[String] =
    Seq(location.entryName, project, dataType.entryName, sampleAlias, version.toString)
}
