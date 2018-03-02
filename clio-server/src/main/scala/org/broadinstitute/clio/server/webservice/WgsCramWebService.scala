package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentWgsCram
import org.broadinstitute.clio.server.service.WgsCramService
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.TransferWgsCramV1Key
import org.broadinstitute.clio.util.model.Location

class WgsCramWebService(wgsCramService: WgsCramService)
    extends IndexWebService[WgsCramIndex.type, DocumentWgsCram](wgsCramService) {

  val servicePathPrefix = "wgscram"

  private[webservice] val pathPrefixKey: Directive1[TransferWgsCramV1Key] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      project <- pathPrefix(Segment)
      sampleAlias <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
    } yield TransferWgsCramV1Key(location, project, sampleAlias, version)
  }
}
