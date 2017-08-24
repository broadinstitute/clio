package org.broadinstitute.clio.client.commands

import java.time.OffsetDateTime

import akka.http.scaladsl.model.HttpResponse
import caseapp.Recurse
import caseapp.core.ArgParser
import io.circe.parser.parse
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.{TransferWgsUbamV1Key, TransferWgsUbamV1Metadata, TransferWgsUbamV1QueryInput}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object CustomArgParsers {
  implicit def locationParser: ArgParser[Location] = {
    ArgParser.instance[Location]("location") { location =>
      if(Location.pathMatcher.contains(location)){
        Right(Location.pathMatcher(location))
      } else {
        Left(s"Unknown location type: $location")
      }
    }
  }
  implicit def documentStatusParser: ArgParser[DocumentStatus] = {
    ArgParser.instance[DocumentStatus]("documentStatus") { documentStatus =>
      val docStatusValue = DocumentStatus.namesToValuesMap.get(documentStatus)
      Either.cond(docStatusValue.isDefined, docStatusValue.get, "Unknown document status.")
    }
  }
  implicit def offsetDateTimeParser: ArgParser[OffsetDateTime] = {
    ArgParser.instance[OffsetDateTime]("date") { offsetDateAndTime =>
      Try(OffsetDateTime.parse(offsetDateAndTime)) match {
        case Success(value) => Right(value)
        case Failure(exception) => Left(exception.getMessage)
      }
    }
  }
}

final case class CommonOptions(bearerToken: String = "")

sealed abstract class Command extends Product with Serializable {
  def execute(webClient: ClioWebClient, bearerToken: String)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse]
}

final case class QueryWgsUbam(
  @Recurse
  transferWgsUbamV1QueryInput: TransferWgsUbamV1QueryInput,
) extends Command {
  def execute(webClient: ClioWebClient, bearerToken: String)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    webClient.queryWgsUbam(
      bearerToken = bearerToken,
      input = transferWgsUbamV1QueryInput
    )
  }
}

final case class AddWgsUbam(metadataLocation: String,
                            @Recurse
                            transferWgsUbamV1Key: TransferWgsUbamV1Key)
    extends Command {
  override def execute(webClient: ClioWebClient, bearerToken: String)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    val decodedOrError = parse(IoUtil.readMetadata(metadataLocation))
      .flatMap(_.as[TransferWgsUbamV1Metadata])

    decodedOrError.fold(Future.failed, { decoded =>
      webClient.addWgsUbam(
        bearerToken = bearerToken,
        transferWgsUbamV1Metadata = decoded,
        input = transferWgsUbamV1Key
      )
    })
  }
}
