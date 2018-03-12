package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.dataaccess.{AuditDAO, MockAuditDAO}
import org.broadinstitute.clio.server.service.AuditService

class MockAuditDirectives(auditDAO: AuditDAO = new MockAuditDAO())
    extends AuditDirectives(AuditService(auditDAO))
