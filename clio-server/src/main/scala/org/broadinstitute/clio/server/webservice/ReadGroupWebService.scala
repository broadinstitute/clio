package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.clio.server.service.ReadGroupService
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._
import org.broadinstitute.clio.transfer.model._

trait ReadGroupWebService {

  def readGroupService: ReadGroupService

  lazy val readGroupRoutes: Route = {
    pathPrefix("readgroup") {
      concat(
        getSchema,
        getSchemaV2,
        postMetadata,
        postMetadataV2,
        query,
        queryV2
      )
    }
  }

  private[webservice] val pathPrefixKeyV1
    : Directive1[TransferReadGroupV1Key] = {
    for {
      flowcellBarcode <- pathPrefix(Segment)
      lane <- pathPrefix(IntNumber)
      libraryName <- pathPrefix(Segment)
    } yield TransferReadGroupV1Key(flowcellBarcode, lane, libraryName)
  }

  private[webservice] val pathPrefixKeyV2
    : Directive1[TransferReadGroupV2Key] = {
    for {
      flowcellBarcode <- pathPrefix(Segment)
      lane <- pathPrefix(IntNumber)
      libraryName <- pathPrefix(Segment)
      location <- pathPrefix(TransferReadGroupLocation.pathMatcher)
    } yield
      TransferReadGroupV2Key(flowcellBarcode, lane, libraryName, location)
  }

  private[webservice] val postMetadata: Route = {
    pathPrefix("metadata") {
      pathPrefix("v1") {
        pathPrefixKeyV1 { key =>
          post {
            entity(as[TransferReadGroupV1Metadata]) { metadata =>
              complete(readGroupService.upsertMetadata(key, metadata))
            }
          }
        }
      }
    }
  }

  private[webservice] val postMetadataV2: Route = {
    pathPrefix("metadata") {
      pathPrefix("v2") {
        pathPrefixKeyV2 { key =>
          post {
            entity(as[TransferReadGroupV1Metadata]) { metadata =>
              complete(readGroupService.upsertMetadataV2(key, metadata))
            }
          }
        }
      }
    }
  }

  private[webservice] val query: Route = {
    pathPrefix("query") {
      pathPrefix("v1") {
        post {
          entity(as[TransferReadGroupV1QueryInput]) { input =>
            complete(readGroupService.queryMetadata(input))
          }
        }
      }
    }
  }

  private[webservice] val queryV2: Route = {
    pathPrefix("query") {
      pathPrefix("v2") {
        post {
          entity(as[TransferReadGroupV2QueryInput]) { input =>
            complete(readGroupService.queryMetadataV2(input))
          }
        }
      }
    }
  }

  private[webservice] val getSchema: Route = {
    pathPrefix("schema") {
      pathPrefix("v1") {
        get {
          complete(readGroupService.querySchema())
        }
      }
    }
  }

  private[webservice] val getSchemaV2: Route = {
    pathPrefix("schema") {
      pathPrefix("v2") {
        get {
          complete(readGroupService.querySchemaV2())
        }
      }
    }
  }
}
