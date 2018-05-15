package org.broadinstitute.clio.client.dispatch

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.generic.extras.Configuration
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation

trait Executor extends LazyLogging with ModelAutoDerivation {

  /**
    * Build a stream which, when pulled, will communicate with the clio-server
    * to update its records about some metadata, potentially performing IO
    * operations in the process.
    */
  def execute(webClient: ClioWebClient, ioUtil: IoUtil): Source[Json, NotUsed]
}

object Executor {

  /** Extension methods enabling use of for-comprehensions on Akka's `Source` type. */
  implicit class SourceMonadOps[A, M](val source: Source[A, M]) extends AnyVal {

    /**
      * Transform every element of the wrapped stream into a new substream using
      * the given function, an concatenate the resulting substreams in-order.
      *
      * Enables using `for` syntax in general.
      */
    def flatMap[B](f: A => Source[B, M]): Source[B, M] = source.flatMapConcat(f)

    /**
      * Build a view of the wrapped stream with the given filter applied.
      *
      * Akka streams don't actually perform any operations until they're run through
      * a materializer, so the built-in `filter` method on `Source` already fits the
      * lazy semantics of `withFilter`.
      *
      * Enables using pattern-matching in the LHS of `for` syntax.
      */
    def withFilter(p: A => Boolean): Source[A, M] = source.filter(p)
  }

  def getJsonKeyFieldNames[CI <: ClioIndex](
    index: ClioIndex
  )(implicit jsonConfig: Configuration): Seq[String] = {
    index.keyMapper.fields.keys.map(jsonConfig.transformMemberNames).toSeq
  }

}
