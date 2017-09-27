package org.broadinstitute.clio.client.commands

import org.broadinstitute.clio.transfer.model._

import caseapp.{CommandName, CommandParser, Recurse}
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

sealed trait GetSchemaCommand extends ClioCommand {
  def index: TransferIndex
}

sealed trait AddCommand extends ClioCommand {
  def index: TransferIndex
  def key: TransferKey
  def metadataLocation: String
}

sealed trait QueryCommand extends ClioCommand {
  def index: TransferIndex
  def queryInput: Any
  def includeDeleted: Boolean
}

// Generic commands.

@CommandName(ClioCommand.getServerHealthName)
case object GetServerHealth extends ClioCommand

@CommandName(ClioCommand.getServerVersionName)
case object GetServerVersion extends ClioCommand

// WGS-uBAM commands.

@CommandName(ClioCommand.getSchemaPrefix + WgsUbamIndex.commandName)
case object GetSchemaWgsUbam extends GetSchemaCommand {
  override def index: TransferIndex = WgsUbamIndex
}

@CommandName(ClioCommand.addPrefix + WgsUbamIndex.commandName)
final case class AddWgsUbam(metadataLocation: String,
                            @Recurse key: TransferWgsUbamV1Key)
    extends AddCommand {
  override def index: TransferIndex = WgsUbamIndex
}

@CommandName(ClioCommand.queryPrefix + WgsUbamIndex.commandName)
final case class QueryWgsUbam(@Recurse queryInput: TransferWgsUbamV1QueryInput,
                              includeDeleted: Boolean = false)
    extends QueryCommand {
  override def index: TransferIndex = WgsUbamIndex
}

@CommandName(ClioCommand.moveWgsUbamName)
final case class MoveWgsUbam(
  @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key,
  destination: String
) extends ClioCommand

@CommandName(ClioCommand.deleteWgsUbamName)
final case class DeleteWgsUbam(
  @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key,
  note: String
) extends ClioCommand

// GVCF commands.

@CommandName(ClioCommand.getSchemaPrefix + GvcfIndex.commandName)
case object GetSchemaGvcf extends GetSchemaCommand {
  override def index: TransferIndex = GvcfIndex
}

@CommandName(ClioCommand.addPrefix + GvcfIndex.commandName)
final case class AddGvcf(metadataLocation: String,
                         @Recurse key: TransferGvcfV1Key)
    extends AddCommand {
  override def index: TransferIndex = GvcfIndex
}

@CommandName(ClioCommand.queryGvcfName)
final case class QueryGvcf(@Recurse queryInput: TransferGvcfV1QueryInput,
                           includeDeleted: Boolean = false)
    extends QueryCommand {
  override def index: TransferIndex = GvcfIndex
}

@CommandName(ClioCommand.moveGvcfName)
final case class MoveGvcf(@Recurse transferGvcfV1Key: TransferGvcfV1Key,
                          destination: String)
    extends ClioCommand

@CommandName(ClioCommand.deleteGvcfName)
final case class DeleteGvcf(@Recurse transferGvcfV1Key: TransferGvcfV1Key,
                            note: String)
    extends ClioCommand

object ClioCommand extends ClioParsers {

  // Names for generic commands.
  val getServerHealthName = "get-server-health"
  val getServerVersionName = "get-server-version"

  val getSchemaPrefix = "get-schema-"
  val addPrefix = "add-"
  val queryPrefix = "query-"
  val movePrefix = "move-"
  val deletePrefix = "delete-"

  // Names for WGS uBAM commands.
  val getWgsUbamSchemaName: String = getSchemaPrefix + WgsUbamIndex.commandName
  val addWgsUbamName: String = addPrefix + WgsUbamIndex.commandName
  val queryWgsUbamName: String = queryPrefix + WgsUbamIndex.commandName
  val moveWgsUbamName: String = movePrefix + WgsUbamIndex.commandName
  val deleteWgsUbamName: String = deletePrefix + WgsUbamIndex.commandName

  // Names for GVCF commands.
  val getGvcfSchemaName: String = getSchemaPrefix + GvcfIndex.commandName
  val addGvcfName: String = addPrefix + GvcfIndex.commandName
  val queryGvcfName: String = queryPrefix + GvcfIndex.commandName
  val moveGvcfName: String = movePrefix + GvcfIndex.commandName
  val deleteGvcfName: String = deletePrefix + GvcfIndex.commandName

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
