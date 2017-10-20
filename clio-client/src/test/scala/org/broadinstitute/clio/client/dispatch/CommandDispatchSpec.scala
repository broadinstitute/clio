package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.util.MockIoUtil
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken

class CommandDispatchSpec extends BaseClientSpec {
  behavior of "CommandDispatch"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return true when we dispatch a valid queryWgsUbam command" in {
    succeedingDispatcher
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
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    succeedingReturningDispatcherWgsUbam(mockIoUtil)
      .dispatch(goodMoveCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid deleteWgsUbam command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)

    succeedingReturningDispatcherWgsUbam(mockIoUtil)
      .dispatch(goodDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid queryGvcf command" in {
    succeedingDispatcher
      .dispatch(goodGvcfQueryCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid addGvcf command" in {
    succeedingDispatcher
      .dispatch(goodGvcfAddCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid moveGvcf command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    mockIoUtil.putFileInCloud(testGvcfSummaryMetricsCloudSourcePath)
    mockIoUtil.putFileInCloud(testGvcfDetailMetricsCloudSourcePath)
    succeedingReturningDispatcherGvcf(mockIoUtil)
      .dispatch(goodGvcfMoveCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid deleteGvcf command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)

    succeedingReturningDispatcherGvcf(mockIoUtil)
      .dispatch(goodGvcfDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid queryWgsCram command" in {
    succeedingDispatcher
      .dispatch(goodCramQueryCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid addWgsCram command" in {
    succeedingDispatcher
      .dispatch(goodCramAddCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid moveWgsCram command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(testCraiCloudSourcePath)
    mockIoUtil.putFileInCloud(testWgsMetricsCloudSourcePath)
    succeedingReturningDispatcherWgsCram(mockIoUtil)
      .dispatch(goodCramMoveCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid deleteWgsCram command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(testCraiCloudSourcePath)

    succeedingReturningDispatcherWgsCram(mockIoUtil)
      .dispatch(goodCramDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }
}
