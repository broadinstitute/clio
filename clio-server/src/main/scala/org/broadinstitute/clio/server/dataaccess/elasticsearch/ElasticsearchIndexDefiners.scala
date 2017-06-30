package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.model.ReadGroup

object ElasticsearchIndexDefiners {
  val ReadGroups =
    new ReflectionIndexDefiner("readgroups", "default", classOf[ReadGroup])

  val All: Seq[ElasticsearchIndexDefiner[_]] = Seq(ReadGroups)
}
