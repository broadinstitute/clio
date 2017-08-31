package org.broadinstitute.client

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.ClioClient
import org.broadinstitute.clio.client.commands.QueryWgsUbam
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class ArgParserSpec extends BaseClientSpec {

  it should "properly parse a Location type " in {
    val parsed = ClioClient.commandParser.detailedParse(
      Array("query-wgs-ubam", "--location", testLocation)
    )(ClioClient.beforeCommandParser)
    val location: Location = parsed match {
      case Right((_, _, optCmd)) =>
        val innerLocation: Location = optCmd.get match {
          case Right((_, query, _, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .transferWgsUbamV1QueryInput
              .location
              .get
          case Left(_) => fail("Could not parse subcommand.")
        }
        innerLocation
      case Left(_) => fail("Could not parse outer command.")
    }
    location should be(Location.pathMatcher(testLocation))
  }

  it should "throw an exception when given an invalid Location type" in {
    val parsed = ClioClient.commandParser.detailedParse(
      Array("query-wgs-ubam", "--location", "BadValue")
    )(ClioClient.beforeCommandParser)
    val errorMessage = parsed match {
      case Right((_, _, optCmd)) =>
        optCmd.get match {
          case Right((_, query, _, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .transferWgsUbamV1QueryInput
              .location
              .get
          case Left(error) => error
        }
      case Left(_) => fail("Could not parse outer command.")
    }
    errorMessage should be(
      "Unknown enum value BadValue for type org.broadinstitute.clio.util.model.Location "
    )
  }

  it should "properly parse a DocumentStatus type" in {
    val parsed = ClioClient.commandParser.detailedParse(
      Array("query-wgs-ubam", "--document-status", testDocumentStatus.toString)
    )(ClioClient.beforeCommandParser)
    val docStatus: DocumentStatus = parsed match {
      case Right((_, _, optCmd)) =>
        val innerDocStatus: DocumentStatus = optCmd.get match {
          case Right((_, query, _, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .transferWgsUbamV1QueryInput
              .documentStatus
              .get
          case Left(_) => fail("Could not parse subcommand.")
        }
        innerDocStatus
      case Left(_) => fail("Could not parse outer command.")
    }
    docStatus should be(testDocumentStatus)
  }

  it should "properly parse an OffsetDateTime string" in {
    val parsed = ClioClient.commandParser.detailedParse(
      Array("query-wgs-ubam", "--run-date-start", testRunDateStart.toString)
    )(ClioClient.beforeCommandParser)
    val runDateStart: OffsetDateTime = parsed match {
      case Right((_, _, optCmd)) =>
        val innerRunDateStart: OffsetDateTime = optCmd.get match {
          case Right((_, query, _, _)) =>
            query
              .asInstanceOf[QueryWgsUbam]
              .transferWgsUbamV1QueryInput
              .runDateStart
              .get
          case Left(_) => fail("Could not parse subcommand.")
        }
        innerRunDateStart
      case Left(_) => fail("Could not parse outer command.")
    }
    runDateStart should be(testRunDateStart)
  }

  it should "properly parse a string into a bearerToken" in {
    val parsed = ClioClient.commandParser.detailedParse(
      Array("--bearer-token", testBearer.token, "query-wgs-ubam")
    )(ClioClient.beforeCommandParser)
    val bearerToken = parsed match {
      case Right((common, _, _)) => common.bearerToken
      case Left(_)               => fail("Could not parse outer command")
    }
    bearerToken should be(testBearer)
  }
}
