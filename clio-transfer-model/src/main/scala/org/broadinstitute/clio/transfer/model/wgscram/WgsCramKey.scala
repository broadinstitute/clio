package org.broadinstitute.clio.transfer.model.wgscram

import org.broadinstitute.clio.util.model.{DataType, Location}

//TODO delete when all other programs have transitioned to new cram API
object WgsCramKey {

  def apply(
    location: Location,
    project: String,
    sampleAlias: String,
    version: Int
  ): WgsCramKey = new WgsCramKey(
    location,
    project,
    sampleAlias,
    version
  )
}

class WgsCramKey(
  override val location: Location,
  override val project: String,
  override val sampleAlias: String,
  override val version: Int
) extends CramKey(location, project, sampleAlias, version, DataType.WGS)
