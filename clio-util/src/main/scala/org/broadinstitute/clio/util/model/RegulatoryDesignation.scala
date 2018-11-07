package org.broadinstitute.clio.util.model

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

sealed abstract class RegulatoryDesignation(val isClinical: Boolean)
    extends EnumEntry
    with UpperSnakecase

object RegulatoryDesignation extends Enum[RegulatoryDesignation] {

  override val values = findValues

  case object ResearchOnly extends RegulatoryDesignation(false)
  case object ClinicalDiagnostics extends RegulatoryDesignation(true)
  case object GeneralCliaCap extends RegulatoryDesignation(true)
}
