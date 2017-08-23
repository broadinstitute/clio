package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import enumeratum._
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{ExecutionContext, Future}

sealed trait CommandType extends EnumEntry

object Commands extends Enum[CommandType] {
  override val values: IndexedSeq[CommandType] = findValues

  case object AddWgsUbam extends CommandType
  case object QueryWgsUbam extends CommandType
  case object MoveWgsUbam extends CommandType

  val pathMatcher = Map(
    AddWgsUbam.toString -> AddWgsUbam,
    QueryWgsUbam.toString -> QueryWgsUbam,
    MoveWgsUbam.toString -> MoveWgsUbam
  )

}

trait Command {
  def execute(webClient: ClioWebClient, config: BaseArgs)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse]
}
