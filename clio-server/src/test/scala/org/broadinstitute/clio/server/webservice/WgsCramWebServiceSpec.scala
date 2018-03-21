package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.MockWgsCramService
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.WgsCramKey
import org.broadinstitute.clio.util.model.Location

class WgsCramWebServiceSpec extends IndexWebServiceSpec[WgsCramIndex.type] {
  def webServiceName = "WgsCramWebService"
  val mockService = new MockWgsCramService()
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
}
