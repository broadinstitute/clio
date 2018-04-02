package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.ArraysIndex
import org.broadinstitute.clio.transfer.model.arrays.ArraysKey
import org.broadinstitute.clio.util.model.Location

class ArraysWebService(arraysService: IndexService[ArraysIndex.type])(
  implicit materializer: Materializer
) extends IndexWebService(arraysService) {

  private[webservice] val pathPrefixKey: Directive1[ArraysKey] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      chipwellBarcode <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
    } yield
      ArraysKey(
        location,
        Symbol(chipwellBarcode),
        version
      )
  }
}
