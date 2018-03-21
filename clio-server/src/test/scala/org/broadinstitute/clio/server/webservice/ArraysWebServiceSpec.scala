package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.MockArraysService
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.ArraysKey
import org.broadinstitute.clio.util.model.Location

class ArraysWebServiceSpec extends IndexWebServiceSpec[ArraysIndex.type] {
  def webServiceName = "ArraysWebService"
  val mockService = new MockArraysService()
  val webService = new ArraysWebService(mockService)

  val onPremKey = ArraysKey(
    Location.OnPrem,
    Symbol("chipwell_barcode"),
    1,
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
}
