package org.broadinstitute.clio.server

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Configuration file accessors.
  */
object ClioConfig {
  // parse dotted names, unlike ConfigFactory.systemEnvironment
  private val environment = ConfigFactory.parseMap(System.getenv)

  /**
    * Enable the passed in config to be overridden via environment variables.
    *
    * NOTE: Mimic of this behavior exists within the ClioIntegrationTestRunner also.
    */
  def withEnvironment(config: Config): Config = {
    environment.withFallback(config).getConfig("clio")
  }

  private val config: Config = withEnvironment(ConfigFactory.load)

  /** Retrieve a clio configuration at a path. */
  def getConfig(path: String): Config = config.getConfig(path)
}
