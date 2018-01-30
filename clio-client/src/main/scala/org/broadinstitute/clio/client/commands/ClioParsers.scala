package org.broadinstitute.clio.client.commands

import java.net.URI
import java.nio.file.{Path, Paths}

import caseapp.core.Error
import caseapp.core.argparser.{ArgParser, SimpleArgParser}
import cats.syntax.either._
import enumeratum.{Enum, EnumEntry}

import scala.reflect.ClassTag
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
    val enum = implicitly[Enum[T]]
    val enumName = c.runtimeClass.getName
    val entryMap = enum.namesToValuesMap
    val entryString = entryMap.keys.mkString(",")

    SimpleArgParser.from[T](entryString) { maybeEntry =>
      entryMap
        .get(maybeEntry)
        .toRight(
          Error.UnrecognizedValue(
            s"Unknown enum value '$maybeEntry' for type $enumName, valid values are [$entryString]"
          )
        )
    }
  }

  private def parseWith[A](label: String, f: String => A): ArgParser[A] = {
    SimpleArgParser.from[A](label) { str =>
      Either
        .catchNonFatal(f(str))
        .leftMap(ex => Error.MalformedValue(label, ex.getMessage))
    }
  }

  implicit val offsetDateTimeParser: ArgParser[OffsetDateTime] =
    parseWith("date", OffsetDateTime.parse)

  implicit val symbolParser: ArgParser[Symbol] =
    parseWith("string", Symbol.apply)

  implicit val uriParser: ArgParser[URI] = parseWith("uri", URI.create)

  implicit val pathParser: ArgParser[Path] = parseWith("path", Paths.get(_))

  implicit val uuidParser: ArgParser[UUID] = parseWith("uuid", UUID.fromString)

  implicit val stringParser: ArgParser[String] =
    SimpleArgParser.from[String]("string") { str =>
      Either.cond(
        !str.contains("\""),
        str,
        Error.MalformedValue("string", "Quotes are not allowed in inputs")
      )
    }
}
