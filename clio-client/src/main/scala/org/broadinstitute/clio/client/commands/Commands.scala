package org.broadinstitute.clio.client.commands

import java.time.OffsetDateTime

import caseapp.Recurse
import caseapp.core.ArgParser
import org.broadinstitute.clio.transfer.model.{TransferWgsUbamV1Key, TransferWgsUbamV1QueryInput}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.util.{Failure, Success, Try}

object CustomArgParsers {
  implicit def locationParser: ArgParser[Location] = {
    ArgParser.instance[Location]("location") { location =>
      if (Location.pathMatcher.contains(location)) {
        Right(Location.pathMatcher(location))
      } else {
        Left(s"Unknown location type: $location")
      }
    }
  }
  implicit def documentStatusParser: ArgParser[DocumentStatus] = {
    ArgParser.instance[DocumentStatus]("documentStatus") { documentStatus =>
      val docStatusValue = DocumentStatus.namesToValuesMap.get(documentStatus)
      Either.cond(
        docStatusValue.isDefined,
        docStatusValue.get,
        "Unknown document status."
      )
    }
  }
  implicit def offsetDateTimeParser: ArgParser[OffsetDateTime] = {
    ArgParser.instance[OffsetDateTime]("date") { offsetDateAndTime =>
      Try(OffsetDateTime.parse(offsetDateAndTime)) match {
        case Success(value)     => Right(value)
        case Failure(exception) => Left(exception.getMessage)
      }
    }
  }
}

final case class CommonOptions(bearerToken: String = "")

sealed trait CommandType

final case class AddWgsUbam(metadataLocation: String,
                            @Recurse
                            transferWgsUbamV1Key: TransferWgsUbamV1Key)
  extends CommandType

final case class QueryWgsUbam(
                               @Recurse
                               transferWgsUbamV1QueryInput: TransferWgsUbamV1QueryInput,
                             ) extends CommandType