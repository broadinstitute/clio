package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.circe.Json
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

/**
  * Executor for all commands that retrieve some data from the
  * clio-server and then print it as JSON.
  */
class RetrieveAndPrintExecutor(command: RetrieveAndPrintCommand, print: String => Unit)
    extends Executor {

  override def execute(
    webClient: ClioWebClient,
    ioUtil: IoUtil
  ): Source[Json, NotUsed] = {
    val responseStream = command match {
      case GetServerHealth  => webClient.getClioServerHealth
      case GetServerVersion => webClient.getClioServerVersion
      case query: QueryCommand[_] =>
        webClient.query(query.index)(query.queryInput, query.includeDeleted)
    }

    responseStream.alsoTo {
      val stringify = Flow.fromFunction[Json, String](_.spaces2).intersperse(",\n")
      val flow = command match {
        case _: QueryCommand[_] =>
          stringify.prepend(Source.single("[")).concat(Source.single("]"))
        case _ => stringify
      }

      flow.concat(Source.single("\n")).to(Sink.foreach(print))
    }
  }
}
