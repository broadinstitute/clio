package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.google.common.jimfs.{Configuration, Jimfs}
import com.sksamuel.elastic4s.Indexable

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import java.nio.file.{FileSystem, Path}

class MemoryPersistenceDAO extends PersistenceDAO {
  val writeCalls: mutable.ArrayBuffer[(_, _)] = mutable.ArrayBuffer.empty

  private val memFS: FileSystem =
    Jimfs.newFileSystem(Configuration.unix())
  override val rootPath: Path = memFS.getPath("/")

  override def writeUpdate[D <: ClioDocument](
    document: D,
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext, indexable: Indexable[D]): Future[Unit] = {
    writeCalls += ((document, index))
    super.writeUpdate(document, index)
  }
}
