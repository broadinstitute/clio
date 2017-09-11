package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.util.MockIoUtil

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken

class CommandDispatchSpec extends BaseClientSpec {
  behavior of "CommandDispatch"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return true when we dispatch a valid queryWgsUbam command" in {
    succeedingDispatcherCamel
      .dispatch(goodQueryCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid addWgsUbam command" in {
    succeedingDispatcher
      .dispatch(goodAddCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid moveWgsUbam command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    succeedingReturningDispatcher(mockIoUtil)
      .dispatch(goodMoveCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid deleteWgsUbam command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)

    succeedingReturningDispatcher(mockIoUtil)
      .dispatch(goodDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }
}