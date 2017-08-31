package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import enumeratum._
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.{FutureWithErrorMessage, IoUtil}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{ExecutionContext, Future}

sealed trait CommandType extends EnumEntry

object Commands extends Enum[CommandType] {
  override val values: IndexedSeq[CommandType] = findValues

  case object AddWgsUbam extends CommandType
  case object QueryWgsUbam extends CommandType
  case object MoveWgsUbam extends CommandType
  case object DeleteWgsUbam extends CommandType

  val pathMatcher = Map(
    AddWgsUbam.toString -> AddWgsUbam,
    QueryWgsUbam.toString -> QueryWgsUbam,
    MoveWgsUbam.toString -> MoveWgsUbam,
    DeleteWgsUbam.toString -> DeleteWgsUbam
  )

}

trait Command
    extends LazyLogging
    with FailFastCirceSupport
    with ModelAutoDerivation
    with FutureWithErrorMessage {
  def execute(webClient: ClioWebClient, config: BaseArgs, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
  ): Future[HttpResponse]
}
