package org.broadinstitute.clio.client.commands

import java.net.URI

import better.files.File
import org.broadinstitute.clio.transfer.model._
import caseapp.{CommandName, Recurse}
import caseapp.core.help.CommandsHelp
import caseapp.core.commandparser.CommandParser
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfKey, GvcfQueryInput}
import org.broadinstitute.clio.transfer.model.wgscram.{WgsCramKey, WgsCramQueryInput}
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamQueryInput}
import org.broadinstitute.clio.transfer.model.arrays.{ArraysKey, ArraysQueryInput}

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

sealed abstract class AddCommand[CI <: ClioIndex](val index: CI) extends ClioCommand {
  def key: index.KeyType
  def metadataLocation: URI
  def force: Boolean
}

sealed abstract class RawQueryCommand[CI <: ClioIndex](val index: CI)
    extends RetrieveAndPrintCommand {
  def queryInputPath: File
}

sealed abstract class SimpleQueryCommand[CI <: ClioIndex](val index: CI)
    extends RetrieveAndPrintCommand {
  def queryInput: index.QueryInputType
  def includeDeleted: Boolean
}

sealed abstract class MoveCommand[+CI <: ClioIndex](val index: CI) extends ClioCommand {
  def key: index.KeyType
  def destination: URI
  def newBasename: Option[String]
}

sealed abstract class DeleteCommand[CI <: ClioIndex](val index: CI) extends ClioCommand {
  def key: index.KeyType
  def note: String
  def force: Boolean
}

sealed abstract class DeliverCommand[+CI <: DeliverableIndex](override val index: CI)
    extends MoveCommand(index) {
  def key: index.KeyType
  def workspaceName: String
  def workspacePath: URI
  final def destination: URI = workspacePath
  def newBasename: Option[String]
  def force: Boolean
}

// Generic commands.

@CommandName(ClioCommand.getServerHealthName)
case object GetServerHealth extends RetrieveAndPrintCommand

@CommandName(ClioCommand.getServerVersionName)
case object GetServerVersion extends RetrieveAndPrintCommand

// WGS-uBAM commands.

@CommandName(ClioCommand.addWgsUbamName)
final case class AddWgsUbam(
  @Recurse key: UbamKey,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(WgsUbamIndex)

@CommandName(ClioCommand.queryWgsUbamName)
final case class QueryWgsUbam(
  @Recurse queryInput: UbamQueryInput,
  includeDeleted: Boolean = false
) extends SimpleQueryCommand(WgsUbamIndex)

@CommandName(ClioCommand.rawQueryWgsUbamName)
final case class RawQueryWgsUbam(
  queryInputPath: File
) extends RawQueryCommand(WgsUbamIndex)

@CommandName(ClioCommand.moveWgsUbamName)
final case class MoveWgsUbam(
  @Recurse key: UbamKey,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(WgsUbamIndex)

@CommandName(ClioCommand.deleteWgsUbamName)
final case class DeleteWgsUbam(
  @Recurse key: UbamKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(WgsUbamIndex)

// GVCF commands.

@CommandName(ClioCommand.addGvcfName)
final case class AddGvcf(
  @Recurse key: GvcfKey,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(GvcfIndex)

@CommandName(ClioCommand.queryGvcfName)
final case class QueryGvcf(
  @Recurse queryInput: GvcfQueryInput,
  includeDeleted: Boolean = false
) extends SimpleQueryCommand(GvcfIndex)

@CommandName(ClioCommand.rawQueryGvcfName)
final case class RawQueryGvcf(
  queryInputPath: File
) extends RawQueryCommand(GvcfIndex)

@CommandName(ClioCommand.moveGvcfName)
final case class MoveGvcf(
  @Recurse key: GvcfKey,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(GvcfIndex)

@CommandName(ClioCommand.deleteGvcfName)
final case class DeleteGvcf(
  @Recurse key: GvcfKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(GvcfIndex)

// WGS-cram commands.

@CommandName(ClioCommand.addWgsCramName)
final case class AddWgsCram(
  @Recurse key: WgsCramKey,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(WgsCramIndex)

@CommandName(ClioCommand.queryWgsCramName)
final case class QueryWgsCram(
  @Recurse queryInput: WgsCramQueryInput,
  includeDeleted: Boolean = false
) extends SimpleQueryCommand(WgsCramIndex)

@CommandName(ClioCommand.rawQueryWgsCramName)
final case class RawQueryWgsCram(
  queryInputPath: File
) extends RawQueryCommand(WgsCramIndex)

@CommandName(ClioCommand.moveWgsCramName)
final case class MoveWgsCram(
  @Recurse key: WgsCramKey,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(WgsCramIndex)

@CommandName(ClioCommand.deleteWgsCramName)
final case class DeleteWgsCram(
  @Recurse key: WgsCramKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(WgsCramIndex)

@CommandName(ClioCommand.deliverWgsCramName)
final case class DeliverWgsCram(
  @Recurse key: WgsCramKey,
  workspaceName: String,
  workspacePath: URI,
  newBasename: Option[String] = None,
  force: Boolean = false
) extends DeliverCommand(WgsCramIndex)

// uBAM commands.

@CommandName(ClioCommand.addUbamName)
final case class AddUbam(
  @Recurse key: UbamKey,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(UbamIndex)

@CommandName(ClioCommand.queryUbamName)
final case class QueryUbam(
  @Recurse queryInput: UbamQueryInput,
  includeDeleted: Boolean = false
) extends SimpleQueryCommand(UbamIndex)

@CommandName(ClioCommand.rawQueryUbamName)
final case class RawQueryUbam(
  queryInputPath: File
) extends RawQueryCommand(UbamIndex)

@CommandName(ClioCommand.moveUbamName)
final case class MoveUbam(
  @Recurse key: UbamKey,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(UbamIndex)

@CommandName(ClioCommand.deleteUbamName)
final case class DeleteUbam(
  @Recurse key: UbamKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(UbamIndex)

// ARRAYS commands.

@CommandName(ClioCommand.addArraysName)
final case class AddArrays(
  @Recurse key: ArraysKey,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(ArraysIndex)

@CommandName(ClioCommand.queryArraysName)
final case class QueryArrays(
  @Recurse queryInput: ArraysQueryInput,
  includeDeleted: Boolean = false
) extends SimpleQueryCommand(ArraysIndex)

@CommandName(ClioCommand.rawQueryArraysName)
final case class RawQueryArrays(
  queryInputPath: File
) extends RawQueryCommand(ArraysIndex)

@CommandName(ClioCommand.moveArraysName)
final case class MoveArrays(
  @Recurse key: ArraysKey,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(ArraysIndex)

@CommandName(ClioCommand.deleteArraysName)
final case class DeleteArrays(
  @Recurse key: ArraysKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(ArraysIndex)

@CommandName(ClioCommand.deliverArraysName)
final case class DeliverArrays(
  @Recurse key: ArraysKey,
  workspaceName: String,
  workspacePath: URI,
  newBasename: Option[String] = None,
  force: Boolean = false
) extends DeliverCommand(ArraysIndex)

object ClioCommand extends ClioParsers {

  // Names for generic commands.
  val getServerHealthName = "get-server-health"
  val getServerVersionName = "get-server-version"

  val addPrefix = "add-"
  val simpleQueryPrefix = "query-"
  val rawQueryPrefix = "raw-query-"
  val movePrefix = "move-"
  val deletePrefix = "delete-"
  val deliverPrefix = "deliver-"

  // Names for WGS uBAM commands.
  val addWgsUbamName: String = addPrefix + WgsUbamIndex.commandName
  val queryWgsUbamName: String = simpleQueryPrefix + WgsUbamIndex.commandName
  val rawQueryWgsUbamName: String = rawQueryPrefix + WgsUbamIndex.commandName
  val moveWgsUbamName: String = movePrefix + WgsUbamIndex.commandName
  val deleteWgsUbamName: String = deletePrefix + WgsUbamIndex.commandName

  // Names for GVCF commands.
  val addGvcfName: String = addPrefix + GvcfIndex.commandName
  val queryGvcfName: String = simpleQueryPrefix + GvcfIndex.commandName
  val rawQueryGvcfName: String = rawQueryPrefix + GvcfIndex.commandName
  val moveGvcfName: String = movePrefix + GvcfIndex.commandName
  val deleteGvcfName: String = deletePrefix + GvcfIndex.commandName

  // Names for WGS cram commands.
  val addWgsCramName: String = addPrefix + WgsCramIndex.commandName
  val queryWgsCramName: String = simpleQueryPrefix + WgsCramIndex.commandName
  val rawQueryWgsCramName: String = rawQueryPrefix + WgsCramIndex.commandName
  val moveWgsCramName: String = movePrefix + WgsCramIndex.commandName
  val deleteWgsCramName: String = deletePrefix + WgsCramIndex.commandName
  val deliverWgsCramName: String = deliverPrefix + WgsCramIndex.commandName

  // Names for uBAM commands.
  val addUbamName: String = addPrefix + UbamIndex.commandName
  val queryUbamName: String = simpleQueryPrefix + UbamIndex.commandName
  val rawQueryUbamName: String = rawQueryPrefix + UbamIndex.commandName
  val moveUbamName: String = movePrefix + UbamIndex.commandName
  val deleteUbamName: String = deletePrefix + UbamIndex.commandName

  // Names for Arrays commands.
  val addArraysName: String = addPrefix + ArraysIndex.commandName
  val queryArraysName: String = simpleQueryPrefix + ArraysIndex.commandName
  val rawQueryArraysName: String = simpleQueryPrefix + ArraysIndex.commandName
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
