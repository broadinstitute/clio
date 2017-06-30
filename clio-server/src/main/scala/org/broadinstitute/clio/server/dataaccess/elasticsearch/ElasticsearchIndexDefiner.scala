package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.model.ElasticsearchIndex

trait ElasticsearchIndexDefiner[A] {
  def indexDefinition: ElasticsearchIndex
}
