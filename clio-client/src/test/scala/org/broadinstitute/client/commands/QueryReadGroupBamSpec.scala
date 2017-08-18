package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.QueryReadGroupBam

class QueryReadGroupBamSpec extends BaseClientSpec {
  behavior of "QueryReadGroupBam"

  it should "return false if there was a server error" in {
    new QueryReadGroupBam(
      clioWebClient = MockClioWebClient.returningInternalError,
      flowcell = Some(testFlowcell),
      lane = Some(testLane),
      libraryName = Some(testLibName),
      location = Some(testLocation),
      lcSet = Some(testLcSet),
      project = Some(testProject),
      sampleAlias = Some(testSampleAlias),
      runDateEnd = Some(testRunDateEnd),
      runDateStart = Some(testRunDateStart),
      bearerToken = testBearer
    ).execute should be(false)
  }

  it should "return true if the server response is OK" in {
    new QueryReadGroupBam(
      clioWebClient = MockClioWebClient.returningOk,
      flowcell = Some(testFlowcell),
      lane = Some(testLane),
      libraryName = Some(testLibName),
      location = Some(testLocation),
      lcSet = Some(testLcSet),
      project = Some(testProject),
      sampleAlias = Some(testSampleAlias),
      runDateEnd = Some(testRunDateEnd),
      runDateStart = Some(testRunDateStart),
      bearerToken = testBearer
    ).execute should be(true)
  }

}
