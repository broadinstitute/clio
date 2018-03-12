package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentGvcf
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1Key
import org.broadinstitute.clio.util.model.Location

class GvcfWebService(gvcfService: IndexService[GvcfIndex.type, DocumentGvcf])
    extends IndexWebService[GvcfIndex.type, DocumentGvcf](gvcfService) {

  private[webservice] val pathPrefixKey: Directive1[TransferGvcfV1Key] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      project <- pathPrefix(Segment)
      sampleAlias <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
    } yield TransferGvcfV1Key(location, project, sampleAlias, version)
  }
}
