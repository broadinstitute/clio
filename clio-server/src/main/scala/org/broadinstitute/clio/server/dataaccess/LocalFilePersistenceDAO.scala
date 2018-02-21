package org.broadinstitute.clio.server.dataaccess

import better.files.File
import org.broadinstitute.clio.server.ClioServerConfig.Persistence

/**
  * Persistence DAO which writes to local disk, for running Clio locally
  * without thrashing GCS in the dev environment.
  */
class LocalFilePersistenceDAO(config: Persistence.LocalConfig, recoveryParallelism: Int)
    extends PersistenceDAO(recoveryParallelism) {

  override lazy val rootPath: File = config.rootDir.getOrElse {
    File.newTemporaryDirectory("clio-persistence").deleteOnExit()
  }
}
