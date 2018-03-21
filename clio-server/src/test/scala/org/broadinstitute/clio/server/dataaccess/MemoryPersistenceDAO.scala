package org.broadinstitute.clio.server.dataaccess

import java.nio.file.FileSystem
import java.time.OffsetDateTime

import better.files.File
import com.google.common.jimfs.{Configuration, Jimfs}
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ClioIndex

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MemoryPersistenceDAO extends PersistenceDAO {

  val writeCalls: mutable.ArrayBuffer[(Json, ElasticsearchIndex[ClioIndex])] =
    mutable.ArrayBuffer.empty

  private val memFS: FileSystem =
    Jimfs.newFileSystem(Configuration.unix())
  override val rootPath: File = File(memFS.getPath("/"))

  override def writeUpdate(
    document: Json,
    index: ElasticsearchIndex[_],
    dt: OffsetDateTime
  )(
    implicit ec: ExecutionContext
  ): Future[Unit] = {
    writeCalls += ((document, index.asInstanceOf[ElasticsearchIndex[ClioIndex]]))
    super.writeUpdate(document, index)
  }
}
