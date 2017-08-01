package org.broadinstitute.clio.client.commands

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferReadGroupLocation,
  TransferReadGroupV1QueryInput
}

class QueryReadGroupBam(clioWebClient: ClioWebClient,
                        flowcell: Option[String],
                        lane: Option[Int],
                        libraryName: Option[String],
                        location: Option[String],
                        lcSet: Option[String],
                        project: Option[String],
                        sampleAlias: Option[String],
                        runDateEnd: Option[OffsetDateTime],
                        runDateStart: Option[OffsetDateTime],
                        bearerToken: String)
    extends Command(clioWebClient = clioWebClient) {

  override def execute: Boolean = {

    val responseFuture = clioWebClient.queryReadGroupBam(
      bearerToken = bearerToken,
      TransferReadGroupV1QueryInput(
        flowcellBarcode = flowcell,
        lane = lane,
        libraryName = libraryName,
        location = location.map(s => TransferReadGroupLocation.pathMatcher(s)),
        lcSet = lcSet,
        project = project,
        sampleAlias = sampleAlias,
        runDateEnd = runDateEnd,
        runDateStart = runDateStart
      )
    )

    checkResponseAndShutdown(Commands.queryReadGroupBam, responseFuture)
  }

}
