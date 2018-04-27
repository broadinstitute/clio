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
      sampleAlias <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
      dataType <- pathPrefix(Segment)
    } yield
      CramKey(
        location,
        project,
        sampleAlias,
        version,
        DataType.withNameInsensitive(dataType)
      )
  }
}