package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.util.model.Location
import org.broadinstitute.clio.server.service.MockWgsUbamService
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.UbamKey

class WgsUbamWebServiceSpec extends IndexWebServiceSpec[WgsUbamIndex.type] {

  def webServiceName = "GvcfWebService"
  val mockService = new MockWgsUbamService()
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
}
