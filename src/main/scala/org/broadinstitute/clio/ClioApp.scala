package org.broadinstitute.clio

import org.broadinstitute.clio.dataaccess.{ElasticsearchDAO, HttpServerDAO}

class ClioApp(val httpServerDAO: HttpServerDAO, val elasticsearchDAO: ElasticsearchDAO)
