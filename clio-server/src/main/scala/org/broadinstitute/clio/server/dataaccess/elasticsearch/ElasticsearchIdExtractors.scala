package org.broadinstitute.clio.dataaccess.elasticsearch

import org.broadinstitute.clio.model.ReadGroup

object ElasticsearchIdExtractors {
  object ReadGroupIdExtractor extends ElasticsearchIdExtractor[ReadGroup] {
    override def idFor(value: ReadGroup): String = {
      s"${value.flowcellBarcode}.${value.lane}.${value.libraryName}"
    }
  }
}
