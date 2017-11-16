package org.broadinstitute.clio.server.dataaccess

import java.nio.file.{FileSystem, Path}

import com.google.common.jimfs.{Configuration, Jimfs}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MemoryPersistenceDAO extends PersistenceDAO {
  val writeCalls: mutable.ArrayBuffer[(_, _)] = mutable.ArrayBuffer.empty

  private val memFS: FileSystem =
    Jimfs.newFileSystem(Configuration.unix())
  override val rootPath: Path = memFS.getPath("/")

  override def writeUpdate[D <: ClioDocument](
    document: D,
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext): Future[Unit] = {
    writeCalls += ((document, index))
    super.writeUpdate(document, index)
  }
}
