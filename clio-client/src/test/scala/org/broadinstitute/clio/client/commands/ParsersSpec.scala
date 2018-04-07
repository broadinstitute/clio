package org.broadinstitute.clio.client.commands

import java.time.OffsetDateTime

import caseapp.core.Error
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class ParsersSpec extends BaseClientSpec {

  private def parse(args: Array[String]) =
    ClioCommand.parser.detailedParse[None.type](args)

  it should "properly parse a Location type " in {
    val parsed =
      parse(Array(ClioCommand.queryWgsUbamName, "--location", Location.GCP.entryName))
    val location: Option[Location] = (parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .queryInput
              .location
          case Left(_) => fail("Could not parse subcommand.")
        }
      case Left(_) => fail("Could not parse outer command.")
    }).flatten

    location should be(Some(Location.GCP))
  }

  it should "throw an exception when given an invalid Location type" in {
    val parsed =
      parse(Array(ClioCommand.queryWgsUbamName, "--location", "BadValue"))
    val errorMessage = parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .queryInput
              .location
          case Left(error) => error
        }
      case Left(_) => fail("Could not parse outer command.")
    }

    val validLocations = Location.namesToValuesMap.keys.mkString(",")

    errorMessage should be(
      Some(
        Error.UnrecognizedValue(
          s"Unknown enum value 'BadValue' for type org.broadinstitute.clio.util.model.Location, valid values are [$validLocations]"
        )
      )
    )
  }

  it should "properly parse a DocumentStatus type" in {
    val parsed = parse(
      Array(
        ClioCommand.queryWgsUbamName,
        "--document-status",
        DocumentStatus.Deleted.entryName
      )
    )
    val docStatus: Option[DocumentStatus] = (parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .queryInput
              .documentStatus
          case Left(_) => fail("Could not parse subcommand.")
        }
      case Left(_) => fail("Could not parse outer command.")
    }).flatten
    docStatus should be(Some(DocumentStatus.Deleted))
  }

  it should "properly parse an OffsetDateTime string" in {
    val now = OffsetDateTime.now()

    val parsed = parse(
      Array(
        ClioCommand.queryWgsUbamName,
        "--run-date-start",
        now.toString
      )
    )
    val runDateStart: Option[OffsetDateTime] = (parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .queryInput
              .runDateStart
          case Left(_) => fail("Could not parse subcommand.")
        }
      case Left(_) => fail("Could not parse outer command.")
    }).flatten
    runDateStart should be(Some(now))
  }

  it should "parse a boolean argument as a flag" in {
    val parsed = parse(Array(ClioCommand.queryWgsUbamName, "--include-deleted"))
    val ignoreDeleted: Boolean = parsed match {
      case Right((_, _, optCmd)) => {
        optCmd map {
          case Right((_, query, _)) =>
            query.asInstanceOf[QueryWgsUbam].includeDeleted
          case Left(_) => fail("Could not parse inner command.")
        }
      }.get
      case Left(_) => fail("Could not parse outer command.")
    }
    ignoreDeleted should be(true)
  }

  it should "error out when there are double quotes on the ends of String inputs" in {
    val parsed =
      parse(Array(ClioCommand.queryWgsUbamName, "--sample-alias", "\"sampleAlias\""))
    val errorMessage = parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _)) =>
            query.asInstanceOf[QueryWgsUbam].queryInput.sampleAlias
          case Left(error) => error
        }
      case Left(_) => fail("Could not parse outer command.")
    }
    errorMessage should be(
      Some(
        Error.MalformedValue(
          "string",
          "Quotes are not allowed in inputs"
        )
      )
    )
  }

  it should "error out when there are double quotes in the middle of String inputs" in {
    val parsed =
      parse(
        Array(
          ClioCommand.queryWgsUbamName,
          "--sample-alias",
          "someone said \"something\" at some point in time"
        )
      )
    val errorMessage = parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _)) =>
            query.asInstanceOf[QueryWgsUbam].queryInput.sampleAlias
          case Left(error) => error
        }
      case Left(_) => fail("Could not parse outer command.")
    }
    errorMessage should be(
      Some(
        Error.MalformedValue(
          "string",
          "Quotes are not allowed in inputs"
        )
      )
    )
  }

  it should "parse single quotes in String inputs without error" in {
    val parsed =
      parse(
        Array(
          ClioCommand.queryWgsUbamName,
          "--sample-alias",
          "I can\'t let you do that, Dave"
        )
      )
    val sampleAlias: Option[String] = (parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .queryInput
              .sampleAlias
          case Left(_) => fail("Could not parse subcommand.")
        }
      case Left(_) => fail("Could not parse outer command.")
    }).flatten

    sampleAlias should be(Some("I can\'t let you do that, Dave"))
  }
}
