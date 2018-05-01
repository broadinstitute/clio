package org.broadinstitute.clio.server.webservice

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.service.CramService
import org.broadinstitute.clio.transfer.model.{BackCompatibleCramIndex, CramIndex}
import org.broadinstitute.clio.transfer.model.wgscram.CramKey
import org.broadinstitute.clio.util.model.{DataType, Location}

class CramWebServiceSpec extends IndexWebServiceSpec[BackCompatibleCramIndex] {
  def webServiceName = "CramWebService"

  val mockService: CramService = mock[CramService]
  val webService = new CramWebService(mockService)

  val onPremKey = CramKey(
    Location.OnPrem,
    "project",
    DataType.WGS,
    "sample_alias",
    1
  )

  val cloudKey: CramKey = onPremKey
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
    CramKey(
      Location.GCP,
      "project",
      DataType.WGS,
      "sample",
      1
    ).asJson(CramIndex.keyEncoder)
  }
}
