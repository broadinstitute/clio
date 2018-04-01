package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.json.ModelAutoDerivation

trait Executor extends LazyLogging with ModelAutoDerivation {

  def execute(webClient: ClioWebClient, ioUtil: IoUtil): Source[Json, NotUsed]
}
