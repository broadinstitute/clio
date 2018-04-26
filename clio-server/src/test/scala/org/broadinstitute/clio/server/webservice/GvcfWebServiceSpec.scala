package org.broadinstitute.clio.server.webservice

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.service.GvcfService
import org.broadinstitute.clio.util.model.{DataType, Location}
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.GvcfKey

class GvcfWebServiceSpec extends IndexWebServiceSpec[GvcfIndex.type] {
  def webServiceName = "GvcfWebService"

  val mockService: GvcfService = mock[GvcfService]
  val webService = new GvcfWebService(mockService)

  val onPremKey = GvcfKey(
    Location.OnPrem,
    "project",
    "sample_alias",
    1,
    DataType.WGS
  )

  val cloudKey: GvcfKey = onPremKey
    .copy(
      location = Location.GCP
    )

  val badMetadataMap = Map(
    "gvcf_size" -> "not longable"
  )

  val badQueryInputMap = Map(
    "version" -> "not intable"
  )

  val emptyOutput: Json =
    GvcfKey(
      Location.GCP,
      "project",
      "sample",
      1,
      DataType.WGS
    ).asJson(GvcfIndex.keyEncoder)
}
