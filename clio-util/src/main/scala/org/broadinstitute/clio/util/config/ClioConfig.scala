package org.broadinstitute.clio.util.config

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

/**
  * Common utilities for parsing configuration from disk / the environment.
  */
object ClioConfig {
  // Parse dotted names, unlike ConfigFactory.systemEnvironment
  private val configFromEnv = ConfigFactory.parseMap(System.getenv)
  private val configFromDisk = ConfigFactory.load

  /**
    * Apply the environment, parsed as a config map, to the given Config
    * object, potentially overriding settings within.
    */
  def withEnvironment(config: Config): Config =
    configFromEnv.withFallback(config)

  /**
    * Load Clio config from disk, apply any overrides that might be present
    * in the environment, and return the "clio" block within the result.
    */
  def load: Config =
    withEnvironment(configFromDisk).as[Config]("clio")
}
