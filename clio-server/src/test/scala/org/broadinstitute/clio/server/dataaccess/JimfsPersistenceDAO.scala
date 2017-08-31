package org.broadinstitute.clio.server.dataaccess

import com.google.common.jimfs.{Configuration, Jimfs}

import java.nio.file.{FileSystem, Path}

/**
  * DAO which persists to an in-memory filesystem.
  */
class JimfsPersistenceDAO extends PersistenceDAO {
  private val memFS: FileSystem =
    Jimfs.newFileSystem(Configuration.unix())
  override val rootPath: Path = memFS.getPath("/")
}
