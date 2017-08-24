package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.CommandDispatch

class CommandDispatchSpec extends BaseClientSpec {
  behavior of "CommandDispatch"

  it should "return true when we dispatch a valid queryWgsUbam command" in {
    CommandDispatch.dispatch(
      MockClioWebClient.returningOk,
      goodQueryCommand,
      testBearer
    ) should be(true)
  }

  it should "return true when we dispatch a valid addWgsUbam command" in {
    CommandDispatch.dispatch(
      MockClioWebClient.returningOk,
      goodAddCommand,
      testBearer
    ) should be(true)
  }

}
