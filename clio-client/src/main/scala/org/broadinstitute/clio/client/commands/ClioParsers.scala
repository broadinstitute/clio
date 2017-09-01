package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import caseapp.core.ArgParser
import enumeratum.{Enum, EnumEntry}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import java.time.OffsetDateTime
import java.util.UUID

/**
  * Argument parsers to use in building command parsers.
  *
  * We use caseapp to auto-magically build command-line parsers for
  * our data types. The docs are sparse, but for what is there see:
  *
  * https://github.com/alexarchambault/case-app/blob/master/README.md
  */
trait ClioParsers {
  implicit def enumEntryParser[T <: EnumEntry: Enum](
    implicit c: ClassTag[T]
  ): ArgParser[T] = {
    ArgParser.instance[T]("entry") { entry =>
      val value = implicitly[Enum[T]].withNameOption(entry)
      Either.cond(
        value.isDefined,
        value.get,
        s"Unknown enum value $entry for type ${c.runtimeClass.getName} "
      )
    }
  }

  implicit val oauthBearerTokenParser: ArgParser[OAuth2BearerToken] = {
    ArgParser.instance[OAuth2BearerToken]("token") { token =>
      //no need for left since this should never fail
      Right(OAuth2BearerToken(token))
    }
  }

  implicit val UUIDParser: ArgParser[UUID] = {
    ArgParser.instance[UUID]("uuid") { uuid =>
      Try(UUID.fromString(uuid)) match {
        case Success(value)     => Right(value)
        case Failure(exception) => Left(exception.getMessage)
      }
    }
  }

  implicit val offsetDateTimeParser: ArgParser[OffsetDateTime] = {
    ArgParser.instance[OffsetDateTime]("date") { offsetDateAndTime =>
      Try(OffsetDateTime.parse(offsetDateAndTime)) match {
        case Success(value)     => Right(value)
        case Failure(exception) => Left(exception.getMessage)
      }
    }
  }
}
