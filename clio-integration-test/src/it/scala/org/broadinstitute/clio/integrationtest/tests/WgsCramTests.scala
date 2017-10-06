package org.broadinstitute.clio.integrationtest.tests

import com.sksamuel.elastic4s.IndexAndType
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

/** Tests of Clio's wgs-cram functionality. */
trait WgsCramTests { self: BaseIntegrationSpec =>
  it should "create the expected wgs-cram mapping in elasticsearch" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val expected = ElasticsearchIndex.WgsCram
    val getRequest =
      getMapping(IndexAndType(expected.indexName, expected.indexType))

    elasticsearchClient.execute(getRequest).map { mappings =>
      mappings should be(Seq(indexToMapping(expected)))
    }
  }
}
