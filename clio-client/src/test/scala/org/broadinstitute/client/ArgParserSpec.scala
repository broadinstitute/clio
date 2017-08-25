package org.broadinstitute.client

import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.ClioClient

class ArgParserSpec extends SystemExitSpec {

  it should "properly parse a Location type " in {
    ClioClient.webClient = MockClioWebClient.returningOk
    val thrown = the [ExitException] thrownBy ClioClient.main(Array("query-wgs-ubam", "--location", testLocation))
    thrown.status should be (0)
  }

  it should "throw an exception when given an invalid Location type" in {
    ClioClient.webClient = MockClioWebClient.returningOk
    val thrown = the [ExitException] thrownBy ClioClient.main(Array("query-wgs-ubam", "--location", "BadValue"))
    thrown.status should be (255)
  }

  it should "properly parse a DocumentStatus type" in {
    ClioClient.webClient = MockClioWebClient.returningOk
    val thrown = the [ExitException] thrownBy ClioClient.main(Array("query-wgs-ubam", "--document-status", testDocumentStatus.toString))
    thrown.status should be (0)
  }

  it should "properly parse an OffsetDateTime string" in {
    ClioClient.webClient = MockClioWebClient.returningOk
    val thrown = the [ExitException] thrownBy ClioClient.main(Array("query-wgs-ubam", "--run-date-start", testRunDateStart.toString))
    thrown.status should be (0)
  }

  it should "properly parse a string into a bearerToken" in {
    ClioClient.webClient = MockClioWebClient.returningOk
    val thrown = the [ExitException] thrownBy ClioClient.main(Array("--bearer-token", testBearer.token, "query-wgs-ubam"))
    thrown.status should be (0)
  }
}
