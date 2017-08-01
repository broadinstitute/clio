package org.broadinstitute.client

import org.broadinstitute.client.webclient.OkReturningMockClioWebClient
import org.broadinstitute.clio.client.ClioClient
import org.broadinstitute.clio.client.webclient.ClioWebClient

object MockClioClient {
  def apply(
    clioWebClient: ClioWebClient = OkReturningMockClioWebClient
  ): ClioClient = {
    new ClioClient(clioWebClient)
  }
}
