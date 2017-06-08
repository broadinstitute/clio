package org.broadinstitute.clio.webservice

import org.broadinstitute.clio.service.AuditService
import org.broadinstitute.clio.{ClioApp, MockClioApp}

class MockAuditDirectives(app: ClioApp = MockClioApp()) extends AuditDirectives {
  override lazy val auditService: AuditService = AuditService(app)
}
