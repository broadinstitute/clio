package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.AuditService
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

class MockAuditDirectives(app: ClioApp = MockClioApp())
    extends AuditDirectives {
  override lazy val auditService: AuditService = AuditService(app)
}
