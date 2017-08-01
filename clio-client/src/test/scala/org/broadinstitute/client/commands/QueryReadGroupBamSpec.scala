package org.broadinstitute.client.commands

import org.broadinstitute.client.util.TestData
import org.broadinstitute.client.webclient.{
  InternalErrorReturningMockClioWebClient,
  OkReturningMockClioWebClient
}
import org.broadinstitute.clio.client.commands.QueryReadGroupBam
import org.scalatest.{AsyncFlatSpec, Matchers}

class QueryReadGroupBamSpec extends AsyncFlatSpec with Matchers with TestData {
  behavior of "QueryReadGroupBam"

  it should "return false if there was a server error" in {
    new QueryReadGroupBam(
      clioWebClient = InternalErrorReturningMockClioWebClient,
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
      clioWebClient = OkReturningMockClioWebClient,
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
