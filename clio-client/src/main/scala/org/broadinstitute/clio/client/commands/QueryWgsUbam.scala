package org.broadinstitute.clio.client.commands

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferWgsUbamV1QueryInput
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.ExecutionContext

class QueryWgsUbam(clioWebClient: ClioWebClient,
                   flowcell: Option[String],
                   lane: Option[Int],
                   libraryName: Option[String],
                   location: Option[String],
                   lcSet: Option[String],
                   project: Option[String],
                   sampleAlias: Option[String],
                   documentStatus: Option[DocumentStatus],
                   runDateEnd: Option[OffsetDateTime],
                   runDateStart: Option[OffsetDateTime],
                   bearerToken: String)(implicit ec: ExecutionContext)
    extends Command(Commands.queryWgsUbam) {

  override def execute: Boolean = {

    val responseFuture = clioWebClient.queryWgsUbam(
      bearerToken = bearerToken,
      TransferWgsUbamV1QueryInput(
        flowcellBarcode = flowcell,
        lane = lane,
        libraryName = libraryName,
        location = location.map(s => Location.pathMatcher(s)),
        lcSet = lcSet,
        project = project,
        sampleAlias = sampleAlias,
        documentStatus = documentStatus,
        runDateEnd = runDateEnd,
        runDateStart = runDateStart
      )
    )

    checkResponse(responseFuture)
  }

}
