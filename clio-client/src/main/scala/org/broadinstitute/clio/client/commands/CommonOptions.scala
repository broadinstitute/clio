package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import caseapp.core.help.Help
import caseapp.core.parser.Parser

/**
  * Common options required for running any Clio CLP.
  *
  * Users will be required to input these options before
  * specifying a command. Any one-letter fields will be
  * treated as short options, while fields with longer
  * names will be treated as long options. It's possible
  * to change this behavior through annotations.
  */
final case class CommonOptions(bearerToken: Option[OAuth2BearerToken] = None)

object CommonOptions extends ClioParsers {

  /** The caseapp parser to use for common options. */
  val parser: Parser[CommonOptions] =
    Parser[CommonOptions]

  /**
    * Common-options-related messages to show when a user
    * asks for help or usage.
    */
  val messages: Help[CommonOptions] =
    Help[CommonOptions]
}
