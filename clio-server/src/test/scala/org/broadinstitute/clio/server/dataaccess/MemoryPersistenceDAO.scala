package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.sksamuel.elastic4s.Indexable

import scala.concurrent.{ExecutionContext, Future}

class MemoryPersistenceDAO extends MockPersistenceDAO {
  var writeCalls: Seq[(_, _)] = Seq.empty

  override def writeUpdate[D <: ClioDocument](
    document: D,
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext, indexable: Indexable[D]): Future[Unit] = {
    writeCalls :+= ((document, index))
    super.writeUpdate(document, index)
  }
}
