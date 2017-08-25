package org.broadinstitute.clio.util.config

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

import java.nio.file.{Path, Paths}

/**
  * Extra config-readers for use across Clio subprojects.
  */
trait ConfigReaders {
  implicit val pathReader: ValueReader[Path] =
    (config: Config, path: String) => Paths.get(config.as[String](path))
}

object ConfigReaders extends ConfigReaders
