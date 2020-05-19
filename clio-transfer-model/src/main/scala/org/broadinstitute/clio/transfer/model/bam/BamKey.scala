package org.broadinstitute.clio.transfer.model.bam

import org.broadinstitute.clio.transfer.model.IndexKey
import org.broadinstitute.clio.util.model.{DataType, Location}

case class BamKey(
  location: Location,
  project: String,
  dataType: DataType,
  sampleAlias: String,
  version: Int
) extends IndexKey {

  override def getUrlSegments: Seq[String] =
    Seq(location.entryName, project, dataType.entryName, sampleAlias, version.toString)
}
