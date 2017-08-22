package org.broadinstitute.clio.client.commands

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.ExecutionContext

object CommandDispatch extends LazyLogging {
  def dispatch(webClient: ClioWebClient,
               config: BaseArgs)(implicit ec: ExecutionContext): Boolean = {

    val commandExecute: Option[Command] = config.command.getOrElse("") match {
      case Commands.`addWgsUbam` =>
        Some(
          new AddWgsUbam(
            clioWebClient = webClient,
            flowcell = config.flowcell.get,
            lane = config.lane.get,
            libraryName = config.libraryName.get,
            location = config.location.get,
            metadataLocation = config.metadataLocation,
            bearerToken = config.bearerToken
          )
        )
      case Commands.`queryWgsUbam` =>
        Some(
          new QueryWgsUbam(
            clioWebClient = webClient,
            bearerToken = config.bearerToken,
            flowcell = config.flowcell,
            lane = config.lane,
            libraryName = config.libraryName,
            location = config.location,
            lcSet = config.lcSet,
            project = config.project,
            sampleAlias = config.sampleAlias,
            documentStatus = config.documentStatus,
            runDateEnd = config.runDateEnd,
            runDateStart = config.runDateStart
          )
        )
      case _ => None
    }

    commandExecute.exists(cmd => cmd.execute)
  }
}
