package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.WgsCramKey
import org.broadinstitute.clio.util.model.Location

class WgsCramWebService(wgsCramService: IndexService[WgsCramIndex.type])
    extends IndexWebService(wgsCramService) {

  private[webservice] val pathPrefixKey: Directive1[WgsCramKey] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      project <- pathPrefix(Segment)
      sampleAlias <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
    } yield WgsCramKey(location, project, sampleAlias, version)
  }
}
