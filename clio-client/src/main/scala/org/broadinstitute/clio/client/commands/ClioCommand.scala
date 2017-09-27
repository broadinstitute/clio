package org.broadinstitute.clio.client.commands

import org.broadinstitute.clio.transfer.model._

import caseapp.{CommandName, Recurse}
import caseapp.core.help.CommandsHelp
import caseapp.core.commandparser.CommandParser

/**
  * A specific operation to perform against Clio.
  *
  * The `@Recurse` annotation tells caseapp to flatten the
  * fields of a nested case class into the option list of
  * the enclosing case class, rather than treat it as a
  * sub-sub-command.
  */
sealed trait ClioCommand

// Generic commands.

@CommandName(ClioCommand.getServerHealthName)
case object GetServerHealth extends ClioCommand

@CommandName(ClioCommand.getServerVersionName)
case object GetServerVersion extends ClioCommand

// WGS-uBAM commands.

@CommandName(ClioCommand.getWgsUbamSchemaName)
case object GetSchemaWgsUbam extends ClioCommand

@CommandName(ClioCommand.addWgsUbamName)
final case class AddWgsUbam(metadataLocation: String,
                            @Recurse transferWgsUbamV1Key: TransferWgsUbamV1Key)
    extends ClioCommand

@CommandName(ClioCommand.queryWgsUbamName)
final case class QueryWgsUbam(
  @Recurse transferWgsUbamV1QueryInput: TransferWgsUbamV1QueryInput,
  includeDeleted: Boolean = false
) extends ClioCommand

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

@CommandName(ClioCommand.getGvcfSchemaName)
case object GetSchemaGvcf extends ClioCommand

@CommandName(ClioCommand.addGvcfName)
final case class AddGvcf(metadataLocation: String,
                         @Recurse transferGvcfV1Key: TransferGvcfV1Key)
    extends ClioCommand

@CommandName(ClioCommand.queryGvcfName)
final case class QueryGvcf(
  @Recurse transferGvcfV1QueryInput: TransferGvcfV1QueryInput,
  includeDeleted: Boolean = false
) extends ClioCommand

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

  // Names for WGS uBAM commands.
  val getWgsUbamSchemaName = "get-schema-wgs-ubam"
  val addWgsUbamName = "add-wgs-ubam"
  val queryWgsUbamName = "query-wgs-ubam"
  val moveWgsUbamName = "move-wgs-ubam"
  val deleteWgsUbamName = "delete-wgs-ubam"

  // Names for GVCF commands.
  val getGvcfSchemaName = "get-schema-gvcf"
  val addGvcfName = "add-gvcf"
  val queryGvcfName = "query-gvcf"
  val moveGvcfName = "move-gvcf"
  val deleteGvcfName = "delete-gvcf"

  /** The caseapp parser to use for all Clio sub-commands. */
  val parser: CommandParser[ClioCommand] =
    CommandParser[ClioCommand]

  /**
    * Sub-command-related messages to show when a users asks
    * for help or usage.
    */
  val help: CommandsHelp[ClioCommand] =
    CommandsHelp[ClioCommand]
}
