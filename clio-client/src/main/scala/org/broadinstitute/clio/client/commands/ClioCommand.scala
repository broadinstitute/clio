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

sealed abstract class GetSchemaCommand[TI <: TransferIndex](
  val index: TransferIndex
) extends ClioCommand

sealed abstract class AddCommand[TI <: TransferIndex](val index: TI)
    extends ClioCommand {
  def key: index.KeyType
  def metadataLocation: String
}

sealed abstract class QueryCommand[TI <: TransferIndex](val index: TI)
    extends ClioCommand {
  def queryInput: index.QueryInputType
  def includeDeleted: Boolean
}

// Generic commands.

@CommandName(ClioCommand.getServerHealthName)
case object GetServerHealth extends ClioCommand

@CommandName(ClioCommand.getServerVersionName)
case object GetServerVersion extends ClioCommand

// WGS-uBAM commands.

@CommandName(ClioCommand.getWgsUbamSchemaName)
case object GetSchemaWgsUbam extends GetSchemaCommand(WgsUbamIndex)

@CommandName(ClioCommand.addWgsUbamName)
final case class AddWgsUbam(metadataLocation: String,
                            @Recurse key: TransferWgsUbamV1Key)
    extends AddCommand(WgsUbamIndex)

@CommandName(ClioCommand.queryWgsUbamName)
final case class QueryWgsUbam(@Recurse queryInput: TransferWgsUbamV1QueryInput,
                              includeDeleted: Boolean = false)
    extends QueryCommand(WgsUbamIndex)

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
case object GetSchemaGvcf extends GetSchemaCommand(GvcfIndex)

@CommandName(ClioCommand.addGvcfName)
final case class AddGvcf(metadataLocation: String,
                         @Recurse key: TransferGvcfV1Key)
    extends AddCommand(GvcfIndex)

@CommandName(ClioCommand.queryGvcfName)
final case class QueryGvcf(@Recurse queryInput: TransferGvcfV1QueryInput,
                           includeDeleted: Boolean = false)
    extends QueryCommand(GvcfIndex)

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
