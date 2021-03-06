package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.GvcfKey
import org.broadinstitute.clio.util.model.{DataType, Location}

class GvcfWebService(gvcfService: IndexService[GvcfIndex.type])
    extends IndexWebService(gvcfService) {

  private[webservice] val pathPrefixKey: Directive1[GvcfKey] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      project <- pathPrefix(Segment)
      dataType <- pathPrefix(DataType.namesToValuesMap)
      sampleAlias <- pathPrefix(Segment)
      version <- path(IntNumber)
    } yield
      GvcfKey(
        location,
        project,
        dataType,
        IndexWebService.decodeSlashes(sampleAlias),
        version
      )
  }
}
