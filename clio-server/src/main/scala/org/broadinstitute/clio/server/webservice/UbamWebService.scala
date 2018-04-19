package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.UbamIndex
import org.broadinstitute.clio.transfer.model.ubam.UbamKey
import org.broadinstitute.clio.util.model.Location

class UbamWebService(ubamService: IndexService[UbamIndex.type])
    extends IndexWebService(ubamService) {

  private[webservice] val pathPrefixKey: Directive1[UbamKey] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      flowcellBarcode <- pathPrefix(Segment)
      lane <- pathPrefix(IntNumber)
      libraryName <- pathPrefix(Segment)
    } yield UbamKey(location, flowcellBarcode, lane, libraryName)
  }
}
