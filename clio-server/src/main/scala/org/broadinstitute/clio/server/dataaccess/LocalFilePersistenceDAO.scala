package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.ClioServerConfig.Persistence

import java.nio.file.{Files, Path}

/**
  * Persistence DAO which writes to local disk, for running Clio locally
  * without thrashing GCS in the dev environment.
  */
class LocalFilePersistenceDAO(config: Persistence.LocalConfig)
    extends PersistenceDAO {
  override lazy val rootPath: Path = config.rootDir.getOrElse {
    Files.createTempDirectory("clio-persistence")
  }
}
