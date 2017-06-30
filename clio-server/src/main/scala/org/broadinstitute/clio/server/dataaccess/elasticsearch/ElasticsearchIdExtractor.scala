package org.broadinstitute.clio.server.dataaccess.elasticsearch

trait ElasticsearchIdExtractor[A] {
  def idFor(value: A): String
}
