package org.broadinstitute.clio.server.webservice

/*
 * These traits just implement Akka-HTTP's header rendering methods.
 * A ModeledCustomHeader implementation will probably want to extend
 * one of these.  RequestResponseHeader is the only one used now for
 * the OidcHeader classes.  See `OidcAccessToken` for example.
 */

/**
  * Headers that render in neither requests nor responses.
  */
trait InternalHeader {
  def renderInRequests(): Boolean = false
  def renderInResponses(): Boolean = false
}

/**
  * Headers that render only in requests.
  */
trait RequestHeader {
  def renderInRequests(): Boolean = true
  def renderInResponses(): Boolean = false
}

/**
  * Headers that render only in responses.
  */
trait ResponseHeader {
  def renderInRequests(): Boolean = false
  def renderInResponses(): Boolean = true
}

/**
  * Headers that render in both requests and responses.
  */
trait RequestResponseHeader {
  def renderInRequests(): Boolean = true
  def renderInResponses(): Boolean = true
}
