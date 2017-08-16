package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.Commands
import org.broadinstitute.clio.client.parser.BaseArgs

class QueryReadGroupBamSpec extends BaseClientSpec {
  behavior of "QueryReadGroupBam"

  val config = BaseArgs(
    flowcell = testFlowcell,
    lane = testLane,
    libraryName = testLibName,
    location = testLocation,
    lcSet = testLcSet,
    project = testProject,
    sampleAlias = testSampleAlias,
    runDateEnd = testRunDateEnd,
    runDateStart = testRunDateStart,
    bearerToken = testBearer
  )

  it should "return false if there was a server error" in {
    Commands.QueryReadGroupBam
      .apply()
      .execute(MockClioWebClient.returningInternalError, config) should be(
      false
    )
  }

  it should "return true if the server response is OK" in {
    Commands.QueryReadGroupBam
      .apply()
      .execute(MockClioWebClient.returningOk, config) should be(true)
  }

}
