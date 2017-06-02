package org.broadinstitute.clio.dataaccess.elasticsearch

import org.broadinstitute.clio.model.ElasticsearchIndex

trait ElasticsearchIndexDefiner[A] {
  def indexDefinition: ElasticsearchIndex
}
