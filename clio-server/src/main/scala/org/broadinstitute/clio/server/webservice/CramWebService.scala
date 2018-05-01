package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.BackCompatibleCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.CramKey
import org.broadinstitute.clio.util.model.{DataType, Location}

class CramWebService(cramService: IndexService[BackCompatibleCramIndex])
    extends IndexWebService(cramService) {

  private[webservice] val pathPrefixKey: Directive1[CramKey] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      project <- pathPrefix(Segment)
      dataType <- pathPrefix(DataType.namesToValuesMap)
      sampleAlias <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
    } yield
      CramKey(
        location,
        project,
        dataType,
        sampleAlias,
        version
      )
  }
}
