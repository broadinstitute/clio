package org.broadinstitute.clio.client.commands

import java.net.URI

import org.broadinstitute.clio.transfer.model._
import caseapp.{CommandName, Recurse}
import caseapp.core.help.CommandsHelp
import caseapp.core.commandparser.CommandParser
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1QueryInput
}
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1QueryInput
}
import org.broadinstitute.clio.transfer.model.wgsubam.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1QueryInput
}

/**
  * A specific operation to perform against Clio.
  *
  * The `@Recurse` annotation tells caseapp to flatten the
  * fields of a nested case class into the option list of
  * the enclosing case class, rather than treat it as a
  * sub-sub-command.
  */
sealed trait ClioCommand

sealed trait RetrieveAndPrintCommand extends ClioCommand

sealed abstract class GetSchemaCommand[TI <: TransferIndex](val index: TI)
    extends RetrieveAndPrintCommand

sealed abstract class AddCommand[TI <: TransferIndex](val index: TI)
    extends ClioCommand {
  def key: index.KeyType
  def metadataLocation: URI
}

sealed abstract class QueryCommand[TI <: TransferIndex](val index: TI)
    extends RetrieveAndPrintCommand {
  def queryInput: index.QueryInputType
  def includeDeleted: Boolean
}

sealed abstract class MoveCommand[TI <: TransferIndex](val index: TI)
    extends ClioCommand {
  def key: index.KeyType
  def destination: URI
  def newPrefix: Option[String] = None
}

sealed abstract class DeleteCommand[TI <: TransferIndex](val index: TI)
    extends ClioCommand {
  def key: index.KeyType
  def note: String
}

// Generic commands.

@CommandName(ClioCommand.getServerHealthName)
case object GetServerHealth extends RetrieveAndPrintCommand

@CommandName(ClioCommand.getServerVersionName)
case object GetServerVersion extends RetrieveAndPrintCommand

// WGS-uBAM commands.

@CommandName(ClioCommand.getWgsUbamSchemaName)
case object GetSchemaWgsUbam extends GetSchemaCommand(WgsUbamIndex)

@CommandName(ClioCommand.addWgsUbamName)
final case class AddWgsUbam(@Recurse key: TransferWgsUbamV1Key,
                            metadataLocation: URI)
    extends AddCommand(WgsUbamIndex)

@CommandName(ClioCommand.queryWgsUbamName)
final case class QueryWgsUbam(@Recurse queryInput: TransferWgsUbamV1QueryInput,
                              includeDeleted: Boolean = false)
    extends QueryCommand(WgsUbamIndex)

@CommandName(ClioCommand.moveWgsUbamName)
final case class MoveWgsUbam(@Recurse key: TransferWgsUbamV1Key,
                             destination: URI)
    extends MoveCommand(WgsUbamIndex)

@CommandName(ClioCommand.deleteWgsUbamName)
final case class DeleteWgsUbam(@Recurse key: TransferWgsUbamV1Key, note: String)
    extends DeleteCommand(WgsUbamIndex)

// GVCF commands.

@CommandName(ClioCommand.getGvcfSchemaName)
case object GetSchemaGvcf extends GetSchemaCommand(GvcfIndex)

@CommandName(ClioCommand.addGvcfName)
final case class AddGvcf(@Recurse key: TransferGvcfV1Key, metadataLocation: URI)
    extends AddCommand(GvcfIndex)

@CommandName(ClioCommand.queryGvcfName)
final case class QueryGvcf(@Recurse queryInput: TransferGvcfV1QueryInput,
                           includeDeleted: Boolean = false)
    extends QueryCommand(GvcfIndex)

@CommandName(ClioCommand.moveGvcfName)
final case class MoveGvcf(@Recurse key: TransferGvcfV1Key, destination: URI)
    extends MoveCommand(GvcfIndex)

@CommandName(ClioCommand.deleteGvcfName)
final case class DeleteGvcf(@Recurse key: TransferGvcfV1Key, note: String)
    extends DeleteCommand(GvcfIndex)

// WGS-cram commands.

@CommandName(ClioCommand.getWgsCramSchemaName)
case object GetSchemaWgsCram extends GetSchemaCommand(WgsCramIndex)

@CommandName(ClioCommand.addWgsCramName)
final case class AddWgsCram(@Recurse key: TransferWgsCramV1Key,
                            metadataLocation: URI)
    extends AddCommand(WgsCramIndex)

@CommandName(ClioCommand.queryWgsCramName)
final case class QueryWgsCram(@Recurse queryInput: TransferWgsCramV1QueryInput,
                              includeDeleted: Boolean = false)
    extends QueryCommand(WgsCramIndex)

@CommandName(ClioCommand.moveWgsCramName)
final case class MoveWgsCram(@Recurse key: TransferWgsCramV1Key,
                             destination: URI,
                             override val newPrefix: Option[String])
    extends MoveCommand(WgsCramIndex)

@CommandName(ClioCommand.deleteWgsCramName)
final case class DeleteWgsCram(@Recurse key: TransferWgsCramV1Key, note: String)
    extends DeleteCommand(WgsCramIndex)

@CommandName(ClioCommand.deliverWgsCramName)
final case class DeliverWgsCram(@Recurse key: TransferWgsCramV1Key,
                                workspaceName: String,
                                workspacePath: URI,
                                newPrefix: Option[String])
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
  val deliverPrefix = "deliver-"

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

  // Names for WGS cram commands.
  val getWgsCramSchemaName: String = getSchemaPrefix + WgsCramIndex.commandName
  val addWgsCramName: String = addPrefix + WgsCramIndex.commandName
  val queryWgsCramName: String = queryPrefix + WgsCramIndex.commandName
  val moveWgsCramName: String = movePrefix + WgsCramIndex.commandName
  val deleteWgsCramName: String = deletePrefix + WgsCramIndex.commandName
  val deliverWgsCramName: String = deliverPrefix + WgsCramIndex.commandName

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
