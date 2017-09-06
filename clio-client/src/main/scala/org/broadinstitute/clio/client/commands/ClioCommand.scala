package org.broadinstitute.clio.client.commands

import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput
}

import caseapp.{CommandParser, Recurse}
import caseapp.core.CommandsMessages
import shapeless.CNil

/**
  * A specific operation to perform against Clio.
  *
  * By default, the caseapp command parser for will convert
  * case class names into kebab case. For example:
  *
  *   AddWgsUbam -> add-wbs-ubam
  *
  * The `@Recurse` annotation tells caseapp to flatten the
  * fields of a nested case class into the option list of
  * the enclosing case class, rather than treat it as a
  * sub-sub-command.
  */
sealed trait ClioCommand

case object GetServerHealth extends ClioCommand
case object GetServerVersion extends ClioCommand

case object GetWgsUbamSchema extends ClioCommand

final case class AddWgsUbam(metadataLocation: String,
                            @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key)
    extends ClioCommand

final case class QueryWgsUbam(
  @Recurse transferWgsUbamV1QueryInput: TransferWgsUbamV1QueryInput,
  includeDeleted: Boolean = false
) extends ClioCommand

final case class MoveWgsUbam(
  @Recurse metadata: TransferWgsUbamV1Metadata,
  @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key
) extends ClioCommand

final case class DeleteWgsUbam(
  @Recurse metadata: TransferWgsUbamV1Metadata,
  @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key
) extends ClioCommand

object ClioCommand extends ClioParsers {

  /*
   * Based on its docs / examples, caseapp *should* be able to inductively
   * derive a `CommandParser[ClioCommand]` without needing this import. For
   * some reason, though, the implicit values defined in the companion object
   * of `CommandParser` aren't getting picked up during implicit search like
   * they should be (on a clean compile in a new JVM at least; something about
   * the Zinc incremental compiler used by the SBT repl and IntelliJ's Scala
   * plugin makes compilation work the second time around, even though if you
   * run "show implicit parameters" on `CommandParser[ClioCommand]` below in
   * IntelliJ without this import it displays a "no implicit found" error message).
   *
   * By importing the implicits from the companion object directly, we get
   * induction to actually work. The `implicitly` call both demonstrates that
   * the base-case implicit is properly in-scope and convinces IntelliJ that
   * the import is actually used for something.
   */
  import caseapp.CommandParser._
  implicitly[CommandParser[CNil]]

  /** The caseapp parser to use for all Clio sub-commands. */
  val parser: CommandParser[ClioCommand] =
    CommandParser[ClioCommand]

  /**
    * Sub-command-related messages to show when a users asks
    * for help or usage.
    */
  val messages: CommandsMessages[ClioCommand] =
    CommandsMessages[ClioCommand]
}
