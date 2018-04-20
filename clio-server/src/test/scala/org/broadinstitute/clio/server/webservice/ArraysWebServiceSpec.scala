package org.broadinstitute.clio.server.webservice

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.service.ArraysService
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.ArraysKey
import org.broadinstitute.clio.util.model.Location

class ArraysWebServiceSpec extends IndexWebServiceSpec[ArraysIndex.type] {
  def webServiceName = "ArraysWebService"

  val mockService: ArraysService = mock[ArraysService]
  val webService = new ArraysWebService(mockService)

  val onPremKey = ArraysKey(
    Location.OnPrem,
    Symbol("chipwell_barcode"),
    1
  )

  val cloudKey: ArraysKey = onPremKey
    .copy(
      location = Location.GCP
    )

  def badMetadataMap = Map(
    "grn_idat" -> "not uriable"
  )

  def badQueryInputMap = Map(
    "version" -> "not intable"
  )

  val emptyOutput: Json = {
    ArraysKey(
      Location.GCP,
      Symbol("chipwell_barcode"),
      1
    ).asJson(ArraysIndex.keyEncoder)
  }
}
