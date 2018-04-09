package org.broadinstitute.clio.transfer.model

import enumeratum._

import scala.collection.immutable

object ApiConstants {
  def forceString: String = "force"
  def apiString: String = "api"
  def queryString: String = "query"
  def rawQueryString: String = "rawquery"
  def metadataString: String = "metadata"
  def healthString: String = "health"
  def versionString: String = "version"
}
sealed trait HealthStatus extends EnumEntry

object HealthStatus extends Enum[HealthStatus] {
  val values: immutable.IndexedSeq[HealthStatus] = findValues

  case object Green extends HealthStatus
  case object Yellow extends HealthStatus
  case object Red extends HealthStatus
}
