package org.broadinstitute.clio.util.json

import java.net.URI

import io.circe.Json
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

trait DecodingUtil extends ModelAutoDerivation {

  def getStringByName(json: Json, name: String): String = {
    json.hcursor
      .get[String](name)
      .fold(throw _, identity)
  }

  def getUriByName(json: Json, name: String): URI = {
    json.hcursor
      .get[URI](name)
      .fold(throw _, identity)
  }

  def getIntByName(json: Json, name: String): Int = {
    json.hcursor.get[Int](name).fold(throw _, identity)
  }

  def getDoubleByName(json: Json, name: String): Double = {
    json.hcursor.get[Double](name).fold(throw _, identity)
  }

  def getLongByName(json: Json, name: String): Long = {
    json.hcursor.get[Long](name).fold(throw _, identity)
  }

  def getDocumentStatus(json: Json): DocumentStatus = {
    json.hcursor
      .get[DocumentStatus]("document_status")
      .fold(throw _, identity)
  }

  def getLocation(json: Json): Location = {
    json.hcursor.get[Location]("location").fold(throw _, identity)
  }
}
