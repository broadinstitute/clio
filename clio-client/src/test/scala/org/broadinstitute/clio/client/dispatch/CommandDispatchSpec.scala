package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.util.MockIoUtil
import akka.http.scaladsl.model.headers.OAuth2BearerToken

class CommandDispatchSpec extends BaseClientSpec {
  behavior of "CommandDispatch"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "not fail when we dispatch a valid queryWgsUbam command" in {
    succeedingDispatcher()
      .dispatch(goodQueryCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid addWgsUbam command" in {
    succeedingDispatcher()
      .dispatch(goodAddCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid moveWgsUbam command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    succeedingDispatcher(mockIoUtil, testWgsUbamLocation)
      .dispatch(goodMoveCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid deleteWgsUbam command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)

    succeedingDispatcher(mockIoUtil, testWgsUbamLocation)
      .dispatch(goodDeleteCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid queryGvcf command" in {
    succeedingDispatcher()
      .dispatch(goodGvcfQueryCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid addGvcf command" in {
    succeedingDispatcher()
      .dispatch(goodGvcfAddCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid moveGvcf command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    mockIoUtil.putFileInCloud(testGvcfSummaryMetricsCloudSourcePath)
    mockIoUtil.putFileInCloud(testGvcfDetailMetricsCloudSourcePath)
    succeedingDispatcher(mockIoUtil, testGvcfLocation)
      .dispatch(goodGvcfMoveCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid deleteGvcf command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)

    succeedingDispatcher(mockIoUtil, testGvcfLocation)
      .dispatch(goodGvcfDeleteCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid queryWgsCram command" in {
    succeedingDispatcher()
      .dispatch(goodCramQueryCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid addWgsCram command" in {
    succeedingDispatcher()
      .dispatch(goodCramAddCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid moveWgsCram command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(testCraiCloudSourcePath)
    succeedingDispatcher(mockIoUtil, testWgsCramLocation)
      .dispatch(goodCramMoveCommand)
      .map(_ => succeed)
  }

  it should "not fail when we dispatch a valid deleteWgsCram command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(testCraiCloudSourcePath)

    succeedingDispatcher(mockIoUtil, testWgsCramLocation)
      .dispatch(goodCramDeleteCommand)
      .map(_ => succeed)
  }
}
