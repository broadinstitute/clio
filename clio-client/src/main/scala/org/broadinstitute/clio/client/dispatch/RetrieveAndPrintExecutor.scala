package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.circe.Json
import io.circe.literal._
import io.circe.parser.parse
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

/**
  * Executor for all commands that retrieve some JSON data from the clio-server and then print it.
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
      case query: RawQueryCommand[_] =>
        parse(query.queryInputPath.contentAsString)
          .fold(
            e =>
              Source.failed(
                new IllegalArgumentException(
                  s"Input file at ${query.queryInputPath} must contain valid JSON.",
                  e
                )
            ),
            webClient.query(query.index)(_, raw = true)
          )
      case query: SimpleQueryCommand[_] =>
        webClient.simpleQuery(query.index)(
          query.queryInput,
          query.includeAllStatuses
        )
    }

    responseStream
      .orElse(Source.single(json"[]"))
      .alsoTo {
        Flow[Json]
          .map(_.spaces2)
          .to(Sink.foreach(print))
      }
  }
}
