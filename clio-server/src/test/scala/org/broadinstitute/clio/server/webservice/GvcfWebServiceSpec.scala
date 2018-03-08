package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentGvcf
import org.broadinstitute.clio.server.service.MockGvcfService
import org.broadinstitute.clio.util.model.Location
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1Key

class GvcfWebServiceSpec extends IndexWebServiceSpec[GvcfIndex.type, DocumentGvcf] {

  def webServiceName = "GvcfWebService"
  val mockService = new MockGvcfService()
  val webService = new GvcfWebService(mockService)

  val onPremKey = TransferGvcfV1Key(
    Location.OnPrem,
    "project",
    "sample_alias",
    1
  )

  val cloudKey: TransferGvcfV1Key = onPremKey
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
