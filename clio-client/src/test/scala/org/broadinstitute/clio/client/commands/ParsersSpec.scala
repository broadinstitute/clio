package org.broadinstitute.clio.client.commands

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import java.time.OffsetDateTime

class ParsersSpec extends BaseClientSpec {

  private def parse(args: Array[String]) =
    ClioCommand.parser.detailedParse(args)(CommonOptions.parser)

  it should "properly parse a Location type " in {
    val parsed = parse(Array(ClioCommand.queryWgsUbamName, "--location", testLocation))
    val location: Option[Location] = (parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .transferWgsUbamV1QueryInput
              .location
          case Left(_) => fail("Could not parse subcommand.")
        }
      case Left(_) => fail("Could not parse outer command.")
    }).flatten

    location should be(Some(Location.pathMatcher(testLocation)))
  }

  it should "throw an exception when given an invalid Location type" in {
    val parsed = parse(Array(ClioCommand.queryWgsUbamName, "--location", "BadValue"))
    val errorMessage = parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .transferWgsUbamV1QueryInput
              .location
          case Left(error) => error
        }
      case Left(_) => fail("Could not parse outer command.")
    }
    errorMessage should be(
      Some(
        "Unknown enum value BadValue for type org.broadinstitute.clio.util.model.Location "
      )
    )
  }

  it should "properly parse a DocumentStatus type" in {
    val parsed = parse(
      Array(ClioCommand.queryWgsUbamName, "--document-status", testDocumentStatus.toString)
    )
    val docStatus: Option[DocumentStatus] = (parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .transferWgsUbamV1QueryInput
              .documentStatus
          case Left(_) => fail("Could not parse subcommand.")
        }
      case Left(_) => fail("Could not parse outer command.")
    }).flatten
    docStatus should be(Some(testDocumentStatus))
  }

  it should "properly parse an OffsetDateTime string" in {
    val parsed = parse(
      Array(ClioCommand.queryWgsUbamName, "--run-date-start", testRunDateStart.toString)
    )
    val runDateStart: Option[OffsetDateTime] = (parsed match {
      case Right((_, _, optCmd)) =>
        optCmd map {
          case Right((_, query, _, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .transferWgsUbamV1QueryInput
              .runDateStart
          case Left(_) => fail("Could not parse subcommand.")
        }
      case Left(_) => fail("Could not parse outer command.")
    }).flatten
    runDateStart should be(Some(testRunDateStart))
  }

  it should "properly parse a string into a bearerToken" in {
    val parsed =
      parse(Array("--bearer-token", testBearer.token, ClioCommand.queryWgsUbamName))
    val bearerToken = parsed match {
      case Right((common, _, _)) => common.bearerToken
      case Left(_)               => fail("Could not parse outer command")
    }
    bearerToken should be(Some(testBearer))
  }

  it should "parse a boolean argument as a flag" in {
    val parsed = parse(Array(ClioCommand.queryWgsUbamName, "--include-deleted"))
    val ignoreDeleted: Boolean = parsed match {
      case Right((_, _, optCmd)) => {
        optCmd map {
          case Right((_, query, _, _)) =>
            query.asInstanceOf[QueryWgsUbam].includeDeleted
          case Left(_) => fail("Could not parse inner command.")
        }
      }.get
      case Left(_) => fail("Could not parse outer command.")
    }
    ignoreDeleted should be(true)
  }
}
