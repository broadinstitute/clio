package org.broadinstitute.clio.dataaccess.elasticsearch

import org.broadinstitute.clio.model.ElasticsearchIndex

trait ElasticSearchIndexDefiner[A] {
  def indexDefinition: ElasticsearchIndex
}
