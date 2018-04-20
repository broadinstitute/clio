package org.broadinstitute.clio.server.webservice

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.service.WgsCramService
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.WgsCramKey
import org.broadinstitute.clio.util.model.Location

class WgsCramWebServiceSpec extends IndexWebServiceSpec[WgsCramIndex.type] {
  def webServiceName = "WgsCramWebService"

  val mockService: WgsCramService = mock[WgsCramService]
  val webService = new WgsCramWebService(mockService)

  val onPremKey = WgsCramKey(
    Location.OnPrem,
    "project",
    "sample_alias",
    1
  )

  val cloudKey: WgsCramKey = onPremKey
    .copy(
      location = Location.GCP
    )

  val badMetadataMap = Map(
    "cram_size" -> "not longable"
  )

  val badQueryInputMap = Map(
    "version" -> "not intable"
  )

  val emptyOutput: Json = {
    WgsCramKey(
      Location.GCP,
      "project",
      "sample",
      1
    ).asJson(WgsCramIndex.keyEncoder)
  }
}
