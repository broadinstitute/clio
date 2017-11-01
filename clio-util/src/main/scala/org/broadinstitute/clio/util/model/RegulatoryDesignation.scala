package org.broadinstitute.clio.util.model

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

sealed trait RegulatoryDesignation extends EnumEntry with UpperSnakecase

object RegulatoryDesignation extends Enum[RegulatoryDesignation] {

  override val values = findValues

  case object ResearchOnly extends RegulatoryDesignation
  case object ClinicalDiagnostics extends RegulatoryDesignation
  case object GeneralCliaCap extends RegulatoryDesignation
}
