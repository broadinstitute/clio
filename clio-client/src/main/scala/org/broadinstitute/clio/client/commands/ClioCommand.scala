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
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1QueryInput
}
import org.broadinstitute.clio.transfer.model.arrays.{
  TransferArraysV1Key,
  TransferArraysV1QueryInput
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

sealed abstract class AddCommand[TI <: TransferIndex](val index: TI) extends ClioCommand {
  def key: index.KeyType
  def metadataLocation: URI
  def force: Boolean
}

sealed abstract class QueryCommand[TI <: TransferIndex](val index: TI)
    extends RetrieveAndPrintCommand {
  def queryInput: index.QueryInputType
  def includeDeleted: Boolean
}

sealed abstract class MoveCommand[+TI <: TransferIndex](val index: TI)
    extends ClioCommand {
  def key: index.KeyType
  def destination: URI
  def newBasename: Option[String]
}

sealed abstract class DeleteCommand[TI <: TransferIndex](val index: TI)
    extends ClioCommand {
  def key: index.KeyType
  def note: String
  def force: Boolean
}

sealed abstract class DeliverCommand[+TI <: TransferIndex](override val index: TI)
    extends MoveCommand(index) {
  def key: index.KeyType
  def workspaceName: String
  def workspacePath: URI
  def destination: URI = workspacePath
  def newBasename: Option[String]
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
final case class AddWgsUbam(
  @Recurse key: TransferUbamV1Key,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(WgsUbamIndex)

@CommandName(ClioCommand.queryWgsUbamName)
final case class QueryWgsUbam(
  @Recurse queryInput: TransferUbamV1QueryInput,
  includeDeleted: Boolean = false
) extends QueryCommand(WgsUbamIndex)

@CommandName(ClioCommand.moveWgsUbamName)
final case class MoveWgsUbam(
  @Recurse key: TransferUbamV1Key,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(WgsUbamIndex)

@CommandName(ClioCommand.deleteWgsUbamName)
final case class DeleteWgsUbam(
  @Recurse key: TransferUbamV1Key,
  note: String,
  force: Boolean = false
) extends DeleteCommand(WgsUbamIndex)

// GVCF commands.

@CommandName(ClioCommand.getGvcfSchemaName)
case object GetSchemaGvcf extends GetSchemaCommand(GvcfIndex)

@CommandName(ClioCommand.addGvcfName)
final case class AddGvcf(
  @Recurse key: TransferGvcfV1Key,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(GvcfIndex)

@CommandName(ClioCommand.queryGvcfName)
final case class QueryGvcf(
  @Recurse queryInput: TransferGvcfV1QueryInput,
  includeDeleted: Boolean = false
) extends QueryCommand(GvcfIndex)

@CommandName(ClioCommand.moveGvcfName)
final case class MoveGvcf(
  @Recurse key: TransferGvcfV1Key,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(GvcfIndex)

@CommandName(ClioCommand.deleteGvcfName)
final case class DeleteGvcf(
  @Recurse key: TransferGvcfV1Key,
  note: String,
  force: Boolean = false
) extends DeleteCommand(GvcfIndex)

// WGS-cram commands.

@CommandName(ClioCommand.getWgsCramSchemaName)
case object GetSchemaWgsCram extends GetSchemaCommand(WgsCramIndex)

@CommandName(ClioCommand.addWgsCramName)
final case class AddWgsCram(
  @Recurse key: TransferWgsCramV1Key,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(WgsCramIndex)

@CommandName(ClioCommand.queryWgsCramName)
final case class QueryWgsCram(
  @Recurse queryInput: TransferWgsCramV1QueryInput,
  includeDeleted: Boolean = false
) extends QueryCommand(WgsCramIndex)

@CommandName(ClioCommand.moveWgsCramName)
final case class MoveWgsCram(
  @Recurse key: TransferWgsCramV1Key,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(WgsCramIndex)

@CommandName(ClioCommand.deleteWgsCramName)
final case class DeleteWgsCram(
  @Recurse key: TransferWgsCramV1Key,
  note: String,
  force: Boolean = false
) extends DeleteCommand(WgsCramIndex)

@CommandName(ClioCommand.deliverWgsCramName)
final case class DeliverWgsCram(
  @Recurse key: TransferWgsCramV1Key,
  workspaceName: String,
  workspacePath: URI,
  newBasename: Option[String]
) extends DeliverCommand(WgsCramIndex)

// Hybsel-uBAM commands.

@CommandName(ClioCommand.getHybselUbamSchemaName)
case object GetSchemaHybselUbam extends GetSchemaCommand(HybselUbamIndex)

@CommandName(ClioCommand.addHybselUbamName)
final case class AddHybselUbam(
  @Recurse key: TransferUbamV1Key,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(HybselUbamIndex)

@CommandName(ClioCommand.queryHybselUbamName)
final case class QueryHybselUbam(
  @Recurse queryInput: TransferUbamV1QueryInput,
  includeDeleted: Boolean = false
) extends QueryCommand(HybselUbamIndex)

@CommandName(ClioCommand.moveHybselUbamName)
final case class MoveHybselUbam(
  @Recurse key: TransferUbamV1Key,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(HybselUbamIndex)

@CommandName(ClioCommand.deleteHybselUbamName)
final case class DeleteHybselUbam(
  @Recurse key: TransferUbamV1Key,
  note: String,
  force: Boolean = false
) extends DeleteCommand(HybselUbamIndex)

// ARRAYS commands.

@CommandName(ClioCommand.getArraysSchemaName)
case object GetSchemaArrays extends GetSchemaCommand(ArraysIndex)

@CommandName(ClioCommand.addArraysName)
final case class AddArrays(
  @Recurse key: TransferArraysV1Key,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(ArraysIndex)

@CommandName(ClioCommand.queryArraysName)
final case class QueryArrays(
  @Recurse queryInput: TransferArraysV1QueryInput,
  includeDeleted: Boolean = false
) extends QueryCommand(ArraysIndex)

@CommandName(ClioCommand.moveArraysName)
final case class MoveArrays(
  @Recurse key: TransferArraysV1Key,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(ArraysIndex)

@CommandName(ClioCommand.deleteArraysName)
final case class DeleteArrays(
  @Recurse key: TransferArraysV1Key,
  note: String,
  force: Boolean = false
) extends DeleteCommand(ArraysIndex)

@CommandName(ClioCommand.deliverArraysName)
final case class DeliverArrays(
  @Recurse key: TransferArraysV1Key,
  workspaceName: String,
  workspacePath: URI,
  newBasename: Option[String]
) extends DeliverCommand(ArraysIndex)

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

  // Names for Hybsel uBAM commands.
  val getHybselUbamSchemaName: String = getSchemaPrefix + HybselUbamIndex.commandName
  val addHybselUbamName: String = addPrefix + HybselUbamIndex.commandName
  val queryHybselUbamName: String = queryPrefix + HybselUbamIndex.commandName
  val moveHybselUbamName: String = movePrefix + HybselUbamIndex.commandName
  val deleteHybselUbamName: String = deletePrefix + HybselUbamIndex.commandName

  // Names for Arrays commands.
  val getArraysSchemaName: String = getSchemaPrefix + ArraysIndex.commandName
  val addArraysName: String = addPrefix + ArraysIndex.commandName
  val queryArraysName: String = queryPrefix + ArraysIndex.commandName
  val moveArraysName: String = movePrefix + ArraysIndex.commandName
  val deleteArraysName: String = deletePrefix + ArraysIndex.commandName
  val deliverArraysName: String = deliverPrefix + ArraysIndex.commandName

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
