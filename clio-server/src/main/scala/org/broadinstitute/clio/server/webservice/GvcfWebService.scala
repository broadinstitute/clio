package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.Materializer
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.GvcfKey
import org.broadinstitute.clio.util.model.Location

class GvcfWebService(gvcfService: IndexService[GvcfIndex.type])(
  implicit materializer: Materializer
) extends IndexWebService(gvcfService) {

  private[webservice] val pathPrefixKey: Directive1[GvcfKey] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      project <- pathPrefix(Segment)
      sampleAlias <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
    } yield GvcfKey(location, project, sampleAlias, version)
  }
}
