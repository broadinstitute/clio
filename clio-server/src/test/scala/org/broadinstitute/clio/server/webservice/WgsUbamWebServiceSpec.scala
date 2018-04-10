package org.broadinstitute.clio.server.webservice

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.util.model.Location
import org.broadinstitute.clio.server.service.WgsUbamService
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.UbamKey

class WgsUbamWebServiceSpec extends IndexWebServiceSpec[WgsUbamIndex.type] {

  def webServiceName = "WgsUbamWebService"

  val mockService: WgsUbamService = mock[WgsUbamService]
  val webService = new WgsUbamWebService(mockService)

  val onPremKey = UbamKey(
    Location.OnPrem,
    "barcode",
    1,
    "library"
  )

  val cloudKey: UbamKey = onPremKey
    .copy(
      location = Location.GCP
    )

  val badMetadataMap = Map(
    "gvcf_size" -> "not longable"
  )

  val badQueryInputMap = Map(
    "version" -> "not intable"
  )

  val emptyOutput: Json = {
    UbamKey(
      Location.GCP,
      "barcode",
      1,
      "library"
    ).asJson(WgsUbamIndex.keyEncoder)
  }
}
