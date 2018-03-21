package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.MockGvcfService
import org.broadinstitute.clio.util.model.Location
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.GvcfKey

class GvcfWebServiceSpec extends IndexWebServiceSpec[GvcfIndex.type] {
  def webServiceName = "GvcfWebService"
  val mockService = new MockGvcfService()
  val webService = new GvcfWebService(mockService)

  val onPremKey = GvcfKey(
    Location.OnPrem,
    "project",
    "sample_alias",
    1
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
}
