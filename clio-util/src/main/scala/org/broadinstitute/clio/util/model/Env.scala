package org.broadinstitute.clio.util.model

import enumeratum.EnumEntry.Lowercase
import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

sealed trait Env extends EnumEntry with Lowercase

object Env extends Enum[Env] {
  override val values: IndexedSeq[Env] = findValues

  case object Dev extends Env
  case object Staging extends Env
  case object Prod extends Env
  // Used by the integration-test runner.
  case object Test extends Env
}
