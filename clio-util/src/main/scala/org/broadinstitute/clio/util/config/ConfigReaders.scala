package org.broadinstitute.clio.util.config

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

import better.files.File

/**
  * Extra config-readers for use across Clio subprojects.
  */
trait ConfigReaders {
  implicit val pathReader: ValueReader[File] =
    (config: Config, path: String) => File(config.as[String](path))
}

object ConfigReaders extends ConfigReaders
