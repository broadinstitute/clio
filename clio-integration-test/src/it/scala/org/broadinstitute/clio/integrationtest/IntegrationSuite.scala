package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.integrationtest.tests.{BasicTests, WgsUbamTests}

/** Convenience trait combining all integration tests, for mixing into our specs. */
trait IntegrationSuite extends BasicTests with WgsUbamTests {
  // Self-type needed here for extending the individual test traits, which have the same self-type.
  self: BaseIntegrationSpec =>
}
