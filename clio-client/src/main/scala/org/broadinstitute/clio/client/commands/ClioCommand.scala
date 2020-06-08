package org.broadinstitute.clio.client.commands

import java.net.URI

import better.files.File
import caseapp.core.commandparser.CommandParser
import caseapp.core.help.CommandsHelp
import caseapp.{CommandName, Recurse}
import org.broadinstitute.clio.client.metadata._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.transfer.model.arrays.{ArraysKey, ArraysQueryInput}
import org.broadinstitute.clio.transfer.model.bam.{BamKey, BamQueryInput}
import org.broadinstitute.clio.transfer.model.cram.{CramKey, CramQueryInput}
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfKey, GvcfQueryInput}
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamQueryInput}

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
  def includeAll: Boolean
  def includeAllStatuses: Boolean = includeDeleted || includeAll
}

sealed abstract class MoveCommand[+CI <: ClioIndex](val index: CI) extends ClioCommand {
  def key: index.KeyType
  def destination: URI
  def newBasename: Option[String]

  def metadataMover: MetadataMover[index.MetadataType]
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
  def billingProject: String
  def workspacePath: URI
  final def destination: URI = workspacePath
  def newBasename: Option[String]
  def force: Boolean
}

sealed abstract class UndeliverCommand[+CI <: DeliverableIndex](override val index: CI)
    extends MoveCommand(index) {
  def key: index.KeyType
  def note: String
  def force: Boolean
  def destination: URI
}

sealed abstract class PatchCommand[CI <: ClioIndex](val index: CI) extends ClioCommand {
  def metadataLocation: URI
  def maxParallelUpserts: Int
}

sealed abstract class MarkExternalCommand[CI <: ClioIndex](val index: CI)
    extends ClioCommand {
  def key: index.KeyType
  def note: String
}

// Generic commands.

@CommandName(ClioCommand.getServerHealthName)
case object GetServerHealth extends RetrieveAndPrintCommand

@CommandName(ClioCommand.getServerVersionName)
case object GetServerVersion extends RetrieveAndPrintCommand

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
  includeDeleted: Boolean = false,
  includeAll: Boolean = false
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
) extends MoveCommand(GvcfIndex) {
  override val metadataMover = new GvcfMover
}

@CommandName(ClioCommand.deleteGvcfName)
final case class DeleteGvcf(
  @Recurse key: GvcfKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(GvcfIndex)

@CommandName(ClioCommand.patchGvcfName)
final case class PatchGvcf(
  metadataLocation: URI,
  maxParallelUpserts: Int = ClioCommand.defaultPatchParallelism
) extends PatchCommand(GvcfIndex)

@CommandName(ClioCommand.markExternalGvcfName)
final case class MarkExternalGvcf(
  @Recurse key: GvcfKey,
  note: String
) extends MarkExternalCommand(GvcfIndex)

// bam commands.

@CommandName(ClioCommand.addBamName)
final case class AddBam(
  @Recurse key: BamKey,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(BamIndex)

@CommandName(ClioCommand.queryBamName)
final case class QueryBam(
  @Recurse queryInput: BamQueryInput,
  includeDeleted: Boolean = false,
  includeAll: Boolean = false
) extends SimpleQueryCommand(BamIndex)

@CommandName(ClioCommand.rawQueryBamName)
final case class RawQueryBam(
  queryInputPath: File
) extends RawQueryCommand(BamIndex)

@CommandName(ClioCommand.moveBamName)
final case class MoveBam(
  @Recurse key: BamKey,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(BamIndex) {
  override val metadataMover = new BamMover
}

@CommandName(ClioCommand.deleteBamName)
final case class DeleteBam(
  @Recurse key: BamKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(BamIndex)

@CommandName(ClioCommand.deliverBamName)
final case class DeliverBam(
  @Recurse key: BamKey,
  workspaceName: String,
  billingProject: String = ClioCommand.defaultBillingProject,
  workspacePath: URI,
  newBasename: Option[String] = None,
  force: Boolean = false,
  deliverSampleMetrics: Boolean = false
) extends DeliverCommand(BamIndex) {
  override val metadataMover = BamDeliverer()
}

@CommandName(ClioCommand.patchBamName)
final case class PatchBam(
  metadataLocation: URI,
  maxParallelUpserts: Int = ClioCommand.defaultPatchParallelism
) extends PatchCommand(BamIndex)

@CommandName(ClioCommand.markExternalBamName)
final case class MarkExternalBam(
  @Recurse key: BamKey,
  note: String
) extends MarkExternalCommand(BamIndex)

// cram commands.

@CommandName(ClioCommand.addCramName)
final case class AddCram(
  @Recurse key: CramKey,
  metadataLocation: URI,
  force: Boolean = false
) extends AddCommand(CramIndex)

@CommandName(ClioCommand.queryCramName)
final case class QueryCram(
  @Recurse queryInput: CramQueryInput,
  includeDeleted: Boolean = false,
  includeAll: Boolean = false
) extends SimpleQueryCommand(CramIndex)

@CommandName(ClioCommand.rawQueryCramName)
final case class RawQueryCram(
  queryInputPath: File
) extends RawQueryCommand(CramIndex)

@CommandName(ClioCommand.moveCramName)
final case class MoveCram(
  @Recurse key: CramKey,
  destination: URI,
  newBasename: Option[String] = None
) extends MoveCommand(CramIndex) {
  override val metadataMover = new CramMover
}

@CommandName(ClioCommand.deleteCramName)
final case class DeleteCram(
  @Recurse key: CramKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(CramIndex)

@CommandName(ClioCommand.deliverCramName)
final case class DeliverCram(
  @Recurse key: CramKey,
  workspaceName: String,
  billingProject: String = ClioCommand.defaultBillingProject,
  workspacePath: URI,
  newBasename: Option[String] = None,
  force: Boolean = false,
  deliverSampleMetrics: Boolean = false
) extends DeliverCommand(CramIndex) {
  override val metadataMover = CramDeliverer(deliverSampleMetrics)
}

@CommandName(ClioCommand.undeliverCramName)
final case class UndeliverCram(
  @Recurse key: CramKey,
  destination: URI,
  force: Boolean = false,
  newBasename: Option[String] = None,
  note: String
) extends UndeliverCommand(CramIndex) {
  override val metadataMover = CramDeliverer(deliverSampleMetrics = false)
}

@CommandName(ClioCommand.patchCramName)
final case class PatchCram(
  metadataLocation: URI,
  maxParallelUpserts: Int = ClioCommand.defaultPatchParallelism
) extends PatchCommand(CramIndex)

@CommandName(ClioCommand.markExternalCramName)
final case class MarkExternalCram(
  @Recurse key: CramKey,
  note: String
) extends MarkExternalCommand(CramIndex)

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
  includeDeleted: Boolean = false,
  includeAll: Boolean = false
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
) extends MoveCommand(UbamIndex) {
  override val metadataMover = new UbamMover
}

@CommandName(ClioCommand.deleteUbamName)
final case class DeleteUbam(
  @Recurse key: UbamKey,
  note: String,
  force: Boolean = false
) extends DeleteCommand(UbamIndex)

@CommandName(ClioCommand.patchUbamName)
final case class PatchUbam(
  metadataLocation: URI,
  maxParallelUpserts: Int = ClioCommand.defaultPatchParallelism
) extends PatchCommand(UbamIndex)

@CommandName(ClioCommand.markExternalUbamName)
final case class MarkExternalUbam(
  @Recurse key: UbamKey,
  note: String
) extends MarkExternalCommand(UbamIndex)

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
  includeDeleted: Boolean = false,
  includeAll: Boolean = false
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
) extends MoveCommand(ArraysIndex) {
  override val metadataMover = new ArrayMover
}

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
  billingProject: String = ClioCommand.defaultBillingProject,
  workspacePath: URI,
  newBasename: Option[String] = None,
  force: Boolean = false
) extends DeliverCommand(ArraysIndex) {
  override val metadataMover = new ArrayDeliverer
}

@CommandName(ClioCommand.undeliverArraysName)
final case class UndeliverArrays(
  @Recurse key: ArraysKey,
  destination: URI,
  note: String,
  newBasename: Option[String] = None,
  force: Boolean = false
) extends UndeliverCommand(ArraysIndex) {
  override val metadataMover = new ArrayUndeliverer
}

@CommandName(ClioCommand.patchArraysName)
final case class PatchArrays(
  metadataLocation: URI,
  maxParallelUpserts: Int = ClioCommand.defaultPatchParallelism
) extends PatchCommand(ArraysIndex)

@CommandName(ClioCommand.markExternalArraysName)
final case class MarkExternalArrays(
  @Recurse key: ArraysKey,
  note: String
) extends MarkExternalCommand(ArraysIndex)

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
  val undeliverPrefix = "undeliver-"
  val patchPrefix = "patch-"
  val markExternalPrefix = "mark-external-"

  // Names for GVCF commands.
  val addGvcfName: String = addPrefix + GvcfIndex.commandName
  val queryGvcfName: String = simpleQueryPrefix + GvcfIndex.commandName
  val rawQueryGvcfName: String = rawQueryPrefix + GvcfIndex.commandName
  val moveGvcfName: String = movePrefix + GvcfIndex.commandName
  val deleteGvcfName: String = deletePrefix + GvcfIndex.commandName
  val patchGvcfName: String = patchPrefix + GvcfIndex.commandName
  val markExternalGvcfName: String = markExternalPrefix + GvcfIndex.commandName

  // Names for bam commands.
  val addBamName: String = addPrefix + BamIndex.commandName
  val queryBamName: String = simpleQueryPrefix + BamIndex.commandName
  val rawQueryBamName: String = rawQueryPrefix + BamIndex.commandName
  val moveBamName: String = movePrefix + BamIndex.commandName
  val deleteBamName: String = deletePrefix + BamIndex.commandName
  val deliverBamName: String = deliverPrefix + BamIndex.commandName
  val patchBamName: String = patchPrefix + BamIndex.commandName
  val markExternalBamName: String = markExternalPrefix + BamIndex.commandName

  // Names for cram commands.
  val addCramName: String = addPrefix + CramIndex.commandName
  val queryCramName: String = simpleQueryPrefix + CramIndex.commandName
  val rawQueryCramName: String = rawQueryPrefix + CramIndex.commandName
  val moveCramName: String = movePrefix + CramIndex.commandName
  val deleteCramName: String = deletePrefix + CramIndex.commandName
  val deliverCramName: String = deliverPrefix + CramIndex.commandName
  val undeliverCramName: String = undeliverPrefix + CramIndex.commandName
  val patchCramName: String = patchPrefix + CramIndex.commandName
  val markExternalCramName: String = markExternalPrefix + CramIndex.commandName

  // Names for uBAM commands.
  val addUbamName: String = addPrefix + UbamIndex.commandName
  val queryUbamName: String = simpleQueryPrefix + UbamIndex.commandName
  val rawQueryUbamName: String = rawQueryPrefix + UbamIndex.commandName
  val moveUbamName: String = movePrefix + UbamIndex.commandName
  val deleteUbamName: String = deletePrefix + UbamIndex.commandName
  val patchUbamName: String = patchPrefix + UbamIndex.commandName
  val markExternalUbamName: String = markExternalPrefix + UbamIndex.commandName

  // Names for Arrays commands.
  val addArraysName: String = addPrefix + ArraysIndex.commandName
  val queryArraysName: String = simpleQueryPrefix + ArraysIndex.commandName
  val rawQueryArraysName: String = rawQueryPrefix + ArraysIndex.commandName
  val moveArraysName: String = movePrefix + ArraysIndex.commandName
  val deleteArraysName: String = deletePrefix + ArraysIndex.commandName
  val deliverArraysName: String = deliverPrefix + ArraysIndex.commandName
  val undeliverArraysName: String = undeliverPrefix + ArraysIndex.commandName
  val patchArraysName: String = patchPrefix + ArraysIndex.commandName
  val markExternalArraysName: String = markExternalPrefix + ArraysIndex.commandName

  /**
    * Default parallelism for patch commands.
    *
    * This choice is pretty arbitrary. 32 worked in prod at one
    * point but overloaded the server on the next attempt, so we
    * dropped it down to 16.
    */
  val defaultPatchParallelism = 16

  val defaultBillingProject = "broad-genomics-data"

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
