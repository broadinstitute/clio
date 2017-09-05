package org.broadinstitute.clio.client.commands

import java.time.OffsetDateTime
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import caseapp.Recurse
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput
}
import caseapp._

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object Commands {

  final case class CommonOptions(bearerToken: Option[OAuth2BearerToken] = None)

  implicit def parser[Options](
    implicit result: CommandParser[Options]
  ): CommandParser[Options] = result

  implicit def enumEntryParser[T <: EnumEntry: Enum](
    implicit c: ClassTag[T]
  ): core.ArgParser[T] = {
    core.ArgParser.instance[T]("entry") { entry =>
      val value = implicitly[Enum[T]].withNameOption(entry)
      Either.cond(
        value.isDefined,
        value.get,
        s"Unknown enum value $entry for type ${c.runtimeClass.getName} "
      )
    }
  }

  implicit def oauthBearerTokenParser: core.ArgParser[OAuth2BearerToken] = {
    core.ArgParser.instance[OAuth2BearerToken]("token") { token =>
      //no need for left since this should never fail
      Right(OAuth2BearerToken(token))
    }
  }

  implicit def UUIDParser: core.ArgParser[UUID] = {
    core.ArgParser.instance[UUID]("uuid") { uuid =>
      Try(UUID.fromString(uuid)) match {
        case Success(value)     => Right(value)
        case Failure(exception) => Left(exception.getMessage)
      }
    }
  }

  implicit def offsetDateTimeParser: core.ArgParser[OffsetDateTime] = {
    core.ArgParser.instance[OffsetDateTime]("date") { offsetDateAndTime =>
      Try(OffsetDateTime.parse(offsetDateAndTime)) match {
        case Success(value)     => Right(value)
        case Failure(exception) => Left(exception.getMessage)
      }
    }
  }
}

sealed trait CommandType

case object GetWgsUbamSchema extends CommandType

final case class AddWgsUbam(metadataLocation: String,
                            @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key)
    extends CommandType

final case class QueryWgsUbam(
  @Recurse transferWgsUbamV1QueryInput: TransferWgsUbamV1QueryInput,
) extends CommandType

final case class MoveWgsUbam(
  @Recurse metadata: TransferWgsUbamV1Metadata,
  @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key
) extends CommandType

final case class DeleteWgsUbam(
  @Recurse metadata: TransferWgsUbamV1Metadata,
  @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key
) extends CommandType
