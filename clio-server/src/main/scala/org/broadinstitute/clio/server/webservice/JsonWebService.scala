package org.broadinstitute.clio.server.webservice

import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import org.broadinstitute.clio.util.json.ModelAutoDerivation

/**
  * Utility trait for combining mixins used across web-service classes.
  */
trait JsonWebService
    extends ModelAutoDerivation
    with ErrorAccumulatingCirceSupport
