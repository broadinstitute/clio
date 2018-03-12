package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentWgsUbam
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.TransferUbamV1Key
import org.broadinstitute.clio.util.model.Location

class WgsUbamWebService(wgsUbamService: IndexService[WgsUbamIndex.type, DocumentWgsUbam])
    extends IndexWebService[WgsUbamIndex.type, DocumentWgsUbam](wgsUbamService) {

  private[webservice] val pathPrefixKey: Directive1[TransferUbamV1Key] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      flowcellBarcode <- pathPrefix(Segment)
      lane <- pathPrefix(IntNumber)
      libraryName <- pathPrefix(Segment)
    } yield TransferUbamV1Key(location, flowcellBarcode, lane, libraryName)
  }
}
