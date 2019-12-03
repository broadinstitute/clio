package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.circe.Json
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

    responseStream.alsoTo {
      val stringify = Flow[Json].map(_.spaces2)
      val flow = command match {
        case _: SimpleQueryCommand[_] | _: RawQueryCommand[_] =>
          /*
           * For queries we expect a stream of multiple elements, so we inject extra
           * characters to be sure the printed stream can be parsed as a JSON array
           * by the caller.
           */
          stringify.intersperse("[", ",\n", "]")
        case _ => stringify
      }

      flow.concat(Source.single("\n")).to(Sink.foreach(print))
    }
  }
}
