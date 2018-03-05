package org.broadinstitute.clio.server.webservice

import java.net.URI

import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.circe.Json
import org.scalatest.{FlatSpec, Matchers}

/**
  * Base class mixing in traits common to all webservice / directive tests.
  */
abstract class BaseWebserviceSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest
    with JsonWebService {

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

  def getDocumentStatus(json: Json): URI = {
    json.hcursor
      .get[URI]("documentStatus")
      .fold(throw _, identity)
  }
}