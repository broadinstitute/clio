package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.BamIndex
import org.broadinstitute.clio.transfer.model.bam.BamKey
import org.broadinstitute.clio.util.model.{DataType, Location}

class BamWebService(bamService: IndexService[BamIndex.type])
    extends IndexWebService(bamService) {

  private[webservice] val pathPrefixKey: Directive1[BamKey] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      project <- pathPrefix(Segment)
      dataType <- pathPrefix(DataType.namesToValuesMap)
      sampleAlias <- pathPrefix(Segment)
      version <- path(IntNumber)
    } yield
      BamKey(
        location,
        project,
        dataType,
        IndexWebService.decodeSlashes(sampleAlias),
        version
      )
  }
}
