package org.broadinstitute.clio.client.commands

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.webclient.ClioWebClient

object CommandDispatch extends LazyLogging {
  def dispatch(webClient: ClioWebClient, config: BaseArgs): Boolean = {
    config.command.exists(_.execute(webClient, config))
  }
}
