package org.broadinstitute.clio.server.webservice

import java.time.OffsetDateTime

object MockRejectionDirectives extends JsonWebService with RejectionDirectives {
  override val serverStartTime: OffsetDateTime = OffsetDateTime.MIN
}
