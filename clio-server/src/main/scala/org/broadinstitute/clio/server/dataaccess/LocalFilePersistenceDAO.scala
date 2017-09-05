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
    val dir = Files.createTempDirectory("clio-persistence").toFile
    dir.deleteOnExit()
    dir.toPath
  }

  override def checkRoot(): Unit = {
    if (!Files.isDirectory(rootPath)) {
      sys.error(s"Local path $rootPath is not a directory, aborting!")
    } else if (!Files.isWritable(rootPath)) {
      sys.error(s"Local path $rootPath is not writeable, aborting!")
    }
  }
}
